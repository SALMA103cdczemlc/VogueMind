"""
VogueMind — Variables explicatives du modèle de réachat (module partagé).

Utilisé à l'identique par l'entraînement (batch/train_model.py) et par l'inférence
temps réel (streaming/predict_stream.py) : les variables sont calculées de la même
façon des deux côtés, ce qui évite tout décalage entraînement / production.

Règle d'or : à une date de coupure `cutoff`, seules les transactions STRICTEMENT
antérieures à `cutoff` sont utilisées (information disponible au moment de la prédiction).
La cible regarde la fenêtre [cutoff ; cutoff + horizon[.
"""
import datetime as dt

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LOOKBACK_DAYS = 28   # fenêtre d'observation du comportement
HORIZON_DAYS = 7     # horizon de prédiction

NUMERIC_FEATURES = [
    "recency_days",      # jours depuis le dernier achat
    "tenure_days",       # jours depuis le premier achat de la fenêtre
    "n_tx_7d", "n_tx_28d",
    "n_days_28d",        # nombre de jours d'achat distincts
    "spend_7d", "spend_28d",
    "avg_price",
    "online_share",      # part des achats en ligne (canal 2)
    "n_articles_28d",
    "n_groups_28d",      # diversité des groupes de produits
    "age",
]
CATEGORICAL_FEATURES = ["club_member_status", "fashion_news_frequency"]


def _d(cutoff):
    return dt.date.fromisoformat(cutoff) if isinstance(cutoff, str) else cutoff


def customer_features(tx: DataFrame, cutoff, articles: DataFrame = None,
                      customers: DataFrame = None) -> DataFrame:
    """Une ligne par client actif dans les LOOKBACK_DAYS jours précédant `cutoff`."""
    c = _d(cutoff)
    start, last7 = c - dt.timedelta(days=LOOKBACK_DAYS), c - dt.timedelta(days=7)
    hist = tx.filter((F.col("t_dat") < F.lit(c)) & (F.col("t_dat") >= F.lit(start)))

    if articles is not None:
        hist = hist.join(articles.select("article_id", "product_group_name"), "article_id", "left")
    else:
        hist = hist.withColumn("product_group_name", F.lit(None).cast("string"))

    in7 = F.col("t_dat") >= F.lit(last7)
    feats = (hist.groupBy("customer_id").agg(
        F.datediff(F.lit(c), F.max("t_dat")).alias("recency_days"),
        F.datediff(F.lit(c), F.min("t_dat")).alias("tenure_days"),
        F.sum(in7.cast("int")).alias("n_tx_7d"),
        F.count("*").alias("n_tx_28d"),
        F.countDistinct("t_dat").alias("n_days_28d"),
        F.sum(F.when(in7, F.col("price")).otherwise(0.0)).alias("spend_7d"),
        F.sum("price").alias("spend_28d"),
        F.avg("price").alias("avg_price"),
        F.avg((F.col("sales_channel_id") == 2).cast("double")).alias("online_share"),
        F.countDistinct("article_id").alias("n_articles_28d"),
        F.countDistinct("product_group_name").alias("n_groups_28d"))
        .withColumn("cutoff", F.lit(c)))

    if customers is not None:
        feats = feats.join(customers.select("customer_id", "age", "club_member_status",
                                            "fashion_news_frequency"), "customer_id", "left")
    else:
        feats = (feats.withColumn("age", F.lit(None).cast("int"))
                 .withColumn("club_member_status", F.lit(None).cast("string"))
                 .withColumn("fashion_news_frequency", F.lit(None).cast("string")))

    return (feats
            .withColumn("age", F.coalesce(F.col("age").cast("double"), F.lit(-1.0)))   # -1 = âge inconnu
            .withColumn("club_member_status", F.coalesce("club_member_status", F.lit("UNKNOWN")))
            .withColumn("fashion_news_frequency",
                        F.coalesce(F.upper("fashion_news_frequency"), F.lit("UNKNOWN"))))


def add_label(feats: DataFrame, tx: DataFrame, cutoff) -> DataFrame:
    """label = 1 si le client achète au moins une fois dans [cutoff ; cutoff + HORIZON_DAYS[."""
    c = _d(cutoff)
    buyers = (tx.filter((F.col("t_dat") >= F.lit(c))
                        & (F.col("t_dat") < F.lit(c + dt.timedelta(days=HORIZON_DAYS))))
              .select("customer_id").distinct().withColumn("label", F.lit(1.0)))
    return feats.join(buyers, "customer_id", "left").fillna({"label": 0.0})


def weekly_cutoffs(first_day, last_day):
    """Coupures hebdomadaires laissant LOOKBACK_DAYS d'historique avant et HORIZON_DAYS après."""
    d, end = _d(first_day) + dt.timedelta(days=LOOKBACK_DAYS), _d(last_day) - dt.timedelta(days=HORIZON_DAYS - 1)
    out = []
    while d <= end:
        out.append(d)
        d += dt.timedelta(days=7)
    return out
