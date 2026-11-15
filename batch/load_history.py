"""
VogueMind — Chargement de l'historique des transactions H&M dans HDFS (voie froide).

Le flux Kafka de la démonstration ne contient que quelques milliers d'événements :
pour entraîner un modèle, Spark a besoin d'un historique de plusieurs semaines.
Ce job lit transactions_train.csv et garde une fenêtre de N semaines.

  Période d'historique (entraînement)   : [start-date ; start-date + weeks[
  Période rejouée ensuite dans Kafka    : à partir de start-date + weeks
  (python ingestion/producer.py --start-date <date affichée par ce job>)

Source (la première trouvée) : hdfs /voguemind/raw/transactions_train.csv, puis data/transactions_train.csv
Sortie : /voguemind/processed/history/transactions/  (Parquet partitionné par t_dat)

Lancement :
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        /opt/voguemind/batch/load_history.py --start-date 2018-09-20 --weeks 12
"""
import argparse
import datetime as dt
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = os.environ.get("VOGUEMIND_HDFS_BASE", "hdfs://namenode:9000/voguemind")
LOCAL_DATA = os.environ.get("VOGUEMIND_LOCAL_DATA", "file:///opt/voguemind/data")
SOURCES = [f"{BASE}/raw/transactions_train.csv", f"{LOCAL_DATA}/transactions_train.csv"]
OUT = f"{BASE}/processed/history/transactions"


def first_existing(spark, paths):
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    for p in paths:
        path = jvm.org.apache.hadoop.fs.Path(p)
        if path.getFileSystem(conf).exists(path):
            return p
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default="2018-09-20", help="premier jour de l'historique (AAAA-MM-JJ)")
    ap.add_argument("--weeks", type=int, default=12, help="durée de l'historique en semaines")
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start_date)
    end = start + dt.timedelta(weeks=args.weeks)          # exclu

    spark = SparkSession.builder.appName("voguemind-load-history").getOrCreate()
    src = first_existing(spark, SOURCES)
    if src is None:
        print("ERREUR : transactions_train.csv introuvable (data/ ou HDFS /voguemind/raw/)")
        return 1

    tx = (spark.read.option("header", True).csv(src)          # tout en texte : garde les zéros initiaux
          .select(F.to_date("t_dat").alias("t_dat"),
                  F.trim("customer_id").alias("customer_id"),
                  F.lpad(F.trim("article_id"), 10, "0").alias("article_id"),
                  F.col("price").cast("double").alias("price"),
                  F.col("sales_channel_id").cast("int").alias("sales_channel_id"))
          .filter((F.col("t_dat") >= F.lit(start)) & (F.col("t_dat") < F.lit(end)))
          .filter(F.col("customer_id").isNotNull() & (F.col("price") > 0) & (F.col("price") <= 1)
                  & F.col("sales_channel_id").isin(1, 2)))

    tx.repartition("t_dat").write.mode("overwrite").partitionBy("t_dat").parquet(OUT)

    hist = spark.read.parquet(OUT)
    stats = hist.agg(F.count("*").alias("transactions"),
                     F.countDistinct("customer_id").alias("clients"),
                     F.countDistinct("article_id").alias("articles"),
                     F.min("t_dat").alias("debut"), F.max("t_dat").alias("fin")).first()
    print(f"\nSource     : {src}")
    print(f"Historique : {stats['debut']} -> {stats['fin']}  ({args.weeks} semaines)")
    print(f"Volume     : {stats['transactions']} transactions, {stats['clients']} clients, "
          f"{stats['articles']} articles")
    print(f"Sortie     : {OUT}")
    print(f"\nPour la démonstration temps réel, rejouer la période suivante :")
    print(f"    python3 ingestion/producer.py --start-date {end.isoformat()} --limit 2000 --delay 0.05")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
