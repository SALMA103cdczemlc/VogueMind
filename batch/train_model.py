"""
VogueMind — Semaine 3 : entraînement du modèle de réachat (classification).

Question métier : « ce client va-t-il racheter chez H&M dans les 7 prochains jours ? »
  - cible  : label = 1 si au moins un achat dans [cutoff ; cutoff + 7 jours[
  - unité  : un client actif (au moins un achat dans les 28 jours précédant cutoff)
  - variables : comportement d'achat sur 28 jours + profil client (batch/features.py)

Découpage TEMPOREL (jamais aléatoire) : coupures hebdomadaires dans l'historique
  - entraînement : toutes les coupures sauf les deux dernières
  - validation   : avant-dernière coupure (comparaison des modèles et réglage)
  - test         : dernière coupure (évaluation finale, jamais vue avant)

Étapes :
  1. référence simple : « a acheté dans les 7 derniers jours => rachète »
  2. trois techniques Spark ML : régression logistique, forêt aléatoire, gradient boosting
  3. réglage (tuning) de la meilleure technique sur la validation
  4. réentraînement sur entraînement + validation, évaluation finale sur le test
  5. sauvegarde du pipeline complet dans HDFS (/voguemind/models/repurchase_<version>)
     + métriques en JSON et Markdown (dossier results/)

Lancement :
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        /opt/voguemind/batch/train_model.py --customer-pct 5 --version v1
"""
import argparse
import datetime as dt
import itertools
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import features as fx

BASE = os.environ.get("VOGUEMIND_HDFS_BASE", "hdfs://namenode:9000/voguemind")
RESULTS_DIR = os.environ.get("VOGUEMIND_RESULTS", "/opt/voguemind/results")
SEED = 42


# --------------------------------------------------------------------------- données
def exists(spark, p):
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(p)
    return path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).exists(path)


def build_dataset(spark, customer_pct):
    tx = spark.read.parquet(f"{BASE}/processed/history/transactions").cache()
    articles = (spark.read.parquet(f"{BASE}/processed/articles")
                if exists(spark, f"{BASE}/processed/articles") else None)
    customers = (spark.read.parquet(f"{BASE}/processed/customers")
                 if exists(spark, f"{BASE}/processed/customers") else None)

    bounds = tx.agg(F.min("t_dat").alias("a"), F.max("t_dat").alias("b")).first()
    cutoffs = fx.weekly_cutoffs(bounds["a"], bounds["b"])
    if len(cutoffs) < 4:
        raise SystemExit(f"Historique trop court ({bounds['a']} -> {bounds['b']}) : "
                         "il faut au moins 4 coupures hebdomadaires (≈ 10 semaines).")

    # Échantillon de clients stable d'une coupure à l'autre (hash du customer_id)
    sample = (F.abs(F.hash("customer_id")) % 100) < customer_pct
    parts = []
    for c in cutoffs:
        f = fx.customer_features(tx, c, articles, customers).filter(sample)
        parts.append(fx.add_label(f, tx, c))
    data = parts[0]
    for p in parts[1:]:
        data = data.unionByName(p)
    # Matérialise le jeu de données une fois (évite de recalculer les variables à chaque modèle)
    data = data.repartition(8).cache()
    data.count()
    tx.unpersist()
    return data, cutoffs, articles is not None, customers is not None


def add_weights(df, pos_rate):
    """Pondération des classes (calculée sur l'entraînement) pour compenser le déséquilibre."""
    w_pos, w_neg = 0.5 / pos_rate, 0.5 / (1 - pos_rate)
    return df.withColumn("weight", F.when(F.col("label") == 1, w_pos).otherwise(w_neg))


# --------------------------------------------------------------------------- modèles
def make_pipeline(classifier):
    """Toutes les transformations sont apprises dans le pipeline => ajustées sur l'entraînement uniquement."""
    idx = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
           for c in fx.CATEGORICAL_FEATURES]
    ohe = OneHotEncoder(inputCols=[f"{c}_idx" for c in fx.CATEGORICAL_FEATURES],
                        outputCols=[f"{c}_ohe" for c in fx.CATEGORICAL_FEATURES], handleInvalid="keep")
    asm = VectorAssembler(inputCols=fx.NUMERIC_FEATURES + [f"{c}_ohe" for c in fx.CATEGORICAL_FEATURES],
                          outputCol="raw_features", handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
    return Pipeline(stages=idx + [ohe, asm, scaler, classifier])


TECHNIQUES = {
    "regression_logistique": (
        lambda **p: LogisticRegression(weightCol="weight", maxIter=100, **p),
        {"regParam": 0.01, "elasticNetParam": 0.0},
        {"regParam": [0.0, 0.01, 0.1], "elasticNetParam": [0.0, 0.5]}),
    "foret_aleatoire": (
        lambda **p: RandomForestClassifier(weightCol="weight", seed=SEED, **p),
        {"numTrees": 40, "maxDepth": 6},
        {"numTrees": [40, 80], "maxDepth": [6, 10], "minInstancesPerNode": [1, 20]}),
    "gradient_boosting": (
        lambda **p: GBTClassifier(weightCol="weight", seed=SEED, **p),
        {"maxIter": 30, "maxDepth": 4},
        {"maxIter": [30, 60], "maxDepth": [3, 5], "stepSize": [0.05, 0.1]}),
}


# --------------------------------------------------------------------------- évaluation
def evaluate(pred, threshold=0.5):
    p = pred.select("label", vector_to_array("probability")[1].alias("p1")) if "probability" in pred.columns \
        else pred.select("label", F.col("prediction").alias("p1"))
    p = p.withColumn("yhat", (F.col("p1") >= threshold).cast("double"))
    cm = p.agg(F.sum(((F.col("yhat") == 1) & (F.col("label") == 1)).cast("int")).alias("tp"),
               F.sum(((F.col("yhat") == 1) & (F.col("label") == 0)).cast("int")).alias("fp"),
               F.sum(((F.col("yhat") == 0) & (F.col("label") == 1)).cast("int")).alias("fn"),
               F.sum(((F.col("yhat") == 0) & (F.col("label") == 0)).cast("int")).alias("tn")).first()
    tp, fp, fn, tn = (int(cm[k] or 0) for k in ("tp", "fp", "fn", "tn"))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    out = {"tp": tp, "fp": fp, "fn": fn, "tn": tn, "precision": round(precision, 4),
           "recall": round(recall, 4), "f1": round(f1, 4),
           "accuracy": round((tp + tn) / max(tp + fp + fn + tn, 1), 4)}
    if "probability" in pred.columns:
        auc = BinaryClassificationEvaluator(rawPredictionCol="probability", metricName="areaUnderROC")
        out["auc"] = round(auc.evaluate(pred), 4)
    return out


def baseline(df):
    """Référence : prédit un réachat si le client a acheté dans les 7 derniers jours."""
    return evaluate(df.withColumn("prediction", (F.col("n_tx_7d") > 0).cast("double")))


def fit_eval(technique, params, train, valid):
    build, _, _ = TECHNIQUES[technique]
    t0 = time.time()
    model = make_pipeline(build(**params)).fit(train)
    m = evaluate(model.transform(valid))
    m["train_seconds"] = round(time.time() - t0, 1)
    return model, m


# --------------------------------------------------------------------------- rapport
def fmt_row(name, m):
    auc = f"{m['auc']:.3f}" if "auc" in m else "—"
    return (f"| {name} | {m['precision']:.3f} | {m['recall']:.3f} | {m['f1']:.3f} | {auc} | "
            f"{m['tp']} | {m['fp']} | {m['fn']} | {m['tn']} |")


def write_report(report, version):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(os.path.join(RESULTS_DIR, f"ml_metrics_{version}.json"), "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    head = "| Modèle | Précision | Rappel | F1 | AUC | VP | FP | FN | VN |\n|---|---|---|---|---|---|---|---|---|"
    lines = [f"# Résultats ML — modèle de réachat {version}", "",
             f"Exécution : {report['run_at']}", "",
             "## Données", "",
             f"- Historique : {report['data']['history_start']} → {report['data']['history_end']}",
             f"- Coupures hebdomadaires : {', '.join(report['data']['cutoffs'])}",
             f"- Entraînement : {report['data']['train_rows']} lignes (coupures {report['data']['train_cutoffs']})",
             f"- Validation : {report['data']['valid_rows']} lignes (coupure {report['data']['valid_cutoff']})",
             f"- Test : {report['data']['test_rows']} lignes (coupure {report['data']['test_cutoff']})",
             f"- Taux de réachat (entraînement) : {report['data']['train_pos_rate']:.1%}",
             f"- Échantillon de clients : {report['data']['customer_pct']} %",
             "", "## Comparaison des techniques (validation)", "", head,
             fmt_row("Référence (achat < 7 j)", report["validation"]["reference"])]
    lines += [fmt_row(k, v) for k, v in report["validation"]["techniques"].items()]
    lines += ["", f"Technique retenue pour le réglage : **{report['tuning']['technique']}**", "",
              "## Réglage (validation)", "", "| Paramètres | F1 | AUC |", "|---|---|---|"]
    lines += [f"| {json.dumps(r['params'])} | {r['f1']:.3f} | {r.get('auc', 0):.3f} |"
              for r in report["tuning"]["grid"]]
    lines += ["", f"Meilleurs paramètres : `{json.dumps(report['tuning']['best_params'])}`", "",
              "## Évaluation finale (test, jamais vu)", "", head,
              fmt_row("Référence (achat < 7 j)", report["test"]["reference"]),
              fmt_row(f"{report['tuning']['technique']} réglé", report["test"]["model"]),
              "", f"Modèle sauvegardé : `{report['model_path']}`", ""]
    with open(os.path.join(RESULTS_DIR, f"ml_metrics_{version}.md"), "w") as fh:
        fh.write("\n".join(lines))
    return "\n".join(lines)


# --------------------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--customer-pct", type=int, default=5, help="% de clients utilisés (1-100)")
    ap.add_argument("--version", default="v1")
    ap.add_argument("--tune", choices=["auto"] + list(TECHNIQUES), default="auto",
                    help="technique à régler (auto = meilleure F1 en validation)")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("voguemind-train-repurchase").getOrCreate()
    data, cutoffs, has_art, has_cust = build_dataset(spark, args.customer_pct)
    train_c, valid_c, test_c = cutoffs[:-2], cutoffs[-2], cutoffs[-1]

    train_raw = data.filter(F.col("cutoff").isin(train_c))
    pos_rate = train_raw.agg(F.avg("label")).first()[0]
    train = add_weights(train_raw, pos_rate).cache()
    valid = add_weights(data.filter(F.col("cutoff") == valid_c), pos_rate).cache()
    test = add_weights(data.filter(F.col("cutoff") == test_c), pos_rate).cache()
    n_train, n_valid, n_test = train.count(), valid.count(), test.count()
    print(f"\nCoupures : {[str(c) for c in cutoffs]}")
    print(f"Entraînement {n_train} | validation {n_valid} | test {n_test} | taux de réachat {pos_rate:.1%}")
    print(f"Référentiels : articles={'oui' if has_art else 'non'}, clients={'oui' if has_cust else 'non'}")

    # 1-2. Référence + trois techniques
    report = {"run_at": dt.datetime.now().isoformat(timespec="seconds"),
              "data": {"history_start": str(cutoffs[0] - dt.timedelta(days=fx.LOOKBACK_DAYS)),
                       "history_end": str(cutoffs[-1] + dt.timedelta(days=fx.HORIZON_DAYS - 1)),
                       "cutoffs": [str(c) for c in cutoffs],
                       "train_cutoffs": ", ".join(str(c) for c in train_c),
                       "valid_cutoff": str(valid_c), "test_cutoff": str(test_c),
                       "train_rows": n_train, "valid_rows": n_valid, "test_rows": n_test,
                       "train_pos_rate": pos_rate, "customer_pct": args.customer_pct,
                       "lookback_days": fx.LOOKBACK_DAYS, "horizon_days": fx.HORIZON_DAYS},
              "validation": {"reference": baseline(valid), "techniques": {}}}
    print("\n=== Comparaison sur la validation ===")
    print(f"  {'référence':<24} {report['validation']['reference']}")
    for name, (_, default_params, _) in TECHNIQUES.items():
        _, m = fit_eval(name, default_params, train, valid)
        report["validation"]["techniques"][name] = m
        print(f"  {name:<24} {m}")

    # 3. Réglage de la technique choisie
    chosen = (max(report["validation"]["techniques"].items(), key=lambda kv: kv[1]["f1"])[0]
              if args.tune == "auto" else args.tune)
    _, _, grid = TECHNIQUES[chosen]
    print(f"\n=== Réglage : {chosen} ===")
    results = []
    for values in itertools.product(*grid.values()):
        params = dict(zip(grid.keys(), values))
        _, m = fit_eval(chosen, params, train, valid)
        results.append({"params": params, **m})
        print(f"  {params} -> F1 {m['f1']:.3f}  AUC {m.get('auc', 0):.3f}")
    best = max(results, key=lambda r: (r["f1"], r.get("auc", 0)))
    report["tuning"] = {"technique": chosen, "grid": results, "best_params": best["params"]}

    # 4. Réentraînement sur entraînement + validation, test final
    full = train.unionByName(valid)
    build, _, _ = TECHNIQUES[chosen]
    final = make_pipeline(build(**best["params"])).fit(full)
    report["test"] = {"reference": baseline(test), "model": evaluate(final.transform(test))}

    # 5. Sauvegarde
    model_path = f"{BASE}/models/repurchase_{args.version}"
    final.write().overwrite().save(model_path)
    report["model_path"] = model_path
    spark.createDataFrame([(json.dumps(report, default=str),)], "report string") \
        .coalesce(1).write.mode("overwrite").text(f"{BASE}/models/repurchase_{args.version}_metrics")

    print("\n" + write_report(report, args.version))
    print(f"Rapport : {RESULTS_DIR}/ml_metrics_{args.version}.md")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
