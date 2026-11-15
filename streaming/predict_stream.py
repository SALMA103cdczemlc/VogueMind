"""
VogueMind — Semaine 4 : voie chaude, prédiction en temps réel.

Au démarrage, le job charge le modèle de réachat sauvegardé dans HDFS
(/voguemind/models/repurchase_<version>), puis, pour chaque micro-batch Kafka :
  1. valide les transactions (mêmes règles que streaming/kafka_to_hdfs.py) ;
  2. recalcule les variables du client avec le MÊME code que l'entraînement
     (batch/features.py), à partir de l'historique HDFS + des nouveaux achats ;
  3. prédit la probabilité que le client rachète dans les 7 jours ;
  4. écrit le résultat dans PostgreSQL (base de restitution, lue par le tableau de bord)
     et l'archive dans HDFS (/voguemind/processed/predictions/).

Reprise : checkpoint dans /voguemind/checkpoints/predict_stream/. Si le job s'arrête,
il repart au dernier offset traité ; la clé primaire PostgreSQL empêche les doublons.

Lancement :
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --conf spark.cores.max=1 /opt/voguemind/streaming/predict_stream.py
"""
import argparse
import datetime as dt
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "batch"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark import StorageLevel
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import features as fx
from kafka_to_hdfs import parse_and_validate

BASE = os.environ.get("VOGUEMIND_HDFS_BASE", "hdfs://namenode:9000/voguemind")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.environ.get("KAFKA_TOPIC", "fashion-transactions")
PG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "voguemind"),
    "user": os.environ.get("POSTGRES_USER", "voguemind"),
    "password": os.environ.get("POSTGRES_PASSWORD", "voguemind"),
}
TX_COLS = ["customer_id", "article_id", "t_dat", "price", "sales_channel_id"]
OUT_COLS = ["transaction_id", "customer_id", "article_id", "t_dat", "price", "sales_channel_id",
            "proba_rachat", "prediction", "model_version", "ingestion_ts", "kafka_ts", "scored_at",
            "batch_id"]


def exists(spark, p):
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(p)
    return path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).exists(path)


def write_postgres(rows):
    """Insertion idempotente (ON CONFLICT DO NOTHING) depuis le driver."""
    import psycopg2
    from psycopg2.extras import execute_values
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        inserted = execute_values(cur, f"""
            INSERT INTO predictions ({", ".join(OUT_COLS)}) VALUES %s
            ON CONFLICT (transaction_id) DO NOTHING RETURNING transaction_id""", rows, fetch=True)
        return len(inserted)


class Scorer:
    def __init__(self, spark, model_path, version, sink):
        self.spark, self.version, self.sink = spark, version, sink
        t0 = time.time()
        self.model = PipelineModel.load(model_path)
        print(f"Modèle chargé : {model_path} ({time.time() - t0:.1f} s)")

        self.articles = (spark.read.parquet(f"{BASE}/processed/articles").cache()
                         if exists(spark, f"{BASE}/processed/articles") else None)
        self.customers = (spark.read.parquet(f"{BASE}/processed/customers").cache()
                          if exists(spark, f"{BASE}/processed/customers") else None)

        # Historique utile : seules les LOOKBACK_DAYS dernières semaines servent aux variables
        hist = spark.read.parquet(f"{BASE}/processed/history/transactions").select(*TX_COLS)
        last = hist.agg(F.max("t_dat")).first()[0]
        keep_from = last - dt.timedelta(days=fx.LOOKBACK_DAYS + 7)
        self.history = hist.filter(F.col("t_dat") >= F.lit(keep_from)) \
            .persist(StorageLevel.MEMORY_AND_DISK)
        print(f"Historique en mémoire : {self.history.count()} transactions depuis {keep_from}")

    def stream_archive(self):
        p = f"{BASE}/raw/transactions"
        return self.spark.read.parquet(p).select("transaction_id", *TX_COLS) if exists(self.spark, p) else None

    def score(self, batch_df: DataFrame, batch_id: int):
        t0 = time.time()
        events = (batch_df.filter(F.col("reject_reason").isNull())
                  .select("transaction_id", *TX_COLS, "ingestion_ts",
                          F.col("kafka_timestamp").alias("kafka_ts"))
                  .dropDuplicates(["transaction_id"]).cache())
        if events.rdd.isEmpty():
            events.unpersist()
            return
        clients = events.select("customer_id").distinct()

        # Historique des clients concernés = historique HDFS + achats déjà reçus en streaming + ce batch
        parts = [self.history.join(F.broadcast(clients), "customer_id").select(*TX_COLS)]
        archive = self.stream_archive()
        if archive is not None:
            parts.append(archive.join(F.broadcast(clients), "customer_id")
                         .join(events.select("transaction_id"), "transaction_id", "left_anti")
                         .select(*TX_COLS))
        parts.append(events.select(*TX_COLS))
        tx = parts[0]
        for p in parts[1:]:
            tx = tx.unionByName(p)
        tx = tx.cache()

        # Un client qui achète le jour d est scoré au lendemain (cutoff = d + 1) :
        # mêmes conventions que l'entraînement.
        scored = []
        for (day,) in events.select("t_dat").distinct().collect():
            cutoff = day + dt.timedelta(days=1)
            day_events = events.filter(F.col("t_dat") == F.lit(day))
            feats = fx.customer_features(tx.join(day_events.select("customer_id").distinct(), "customer_id"),
                                         cutoff, self.articles, self.customers)
            pred = (self.model.transform(feats)
                    .select("customer_id",
                            vector_to_array("probability")[1].alias("proba_rachat"),
                            F.col("prediction").cast("int").alias("prediction")))
            scored.append(day_events.join(pred, "customer_id", "left"))
        out = scored[0]
        for s in scored[1:]:
            out = out.unionByName(s)
        out = (out.fillna({"proba_rachat": 0.0, "prediction": 0})
               .withColumn("model_version", F.lit(self.version))
               .withColumn("scored_at", F.current_timestamp())
               .withColumn("batch_id", F.lit(batch_id))
               .select(*OUT_COLS).cache())

        out.write.mode("append").parquet(f"{BASE}/processed/predictions")
        rows = [tuple(r) for r in out.collect()]
        inserted = write_postgres(rows) if self.sink == "postgres" else len(rows)
        n_pos = sum(1 for r in rows if r[OUT_COLS.index("prediction")] == 1)
        print(f"[batch {batch_id}] {len(rows)} transactions scorées "
              f"({n_pos} réachats prédits), {inserted} nouvelles lignes en base, "
              f"{time.time() - t0:.1f} s", flush=True)
        for df in (out, tx, events):
            df.unpersist()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", default="v1", help="version du modèle (models/repurchase_<version>)")
    ap.add_argument("--once", action="store_true", help="traite les messages disponibles puis s'arrête")
    ap.add_argument("--interval", default="5 seconds")
    ap.add_argument("--max-offsets", type=int, default=2000)
    ap.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"],
                    help="latest = seulement les nouveaux événements (démo temps réel)")
    ap.add_argument("--sink", default="postgres", choices=["postgres", "hdfs"])
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("voguemind-predict-stream")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    scorer = Scorer(spark, f"{BASE}/models/repurchase_{args.version}", args.version, args.sink)

    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", TOPIC)
              .option("startingOffsets", args.starting_offsets)
              .option("maxOffsetsPerTrigger", args.max_offsets)
              .option("failOnDataLoss", "false")
              .load())
    writer = (parse_and_validate(source).writeStream
              .foreachBatch(scorer.score)
              .option("checkpointLocation", f"{BASE}/checkpoints/predict_stream")
              .queryName("predict_stream"))
    writer = writer.trigger(availableNow=True) if args.once else writer.trigger(processingTime=args.interval)
    print(f"En écoute sur {BOOTSTRAP} / {TOPIC} ...", flush=True)
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()
