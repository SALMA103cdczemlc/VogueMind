"""
VogueMind — Nettoyage de l'historique des transactions archivé par le streaming.

Entrée  : /voguemind/raw/transactions/        (écrit par streaming/kafka_to_hdfs.py)
Sortie  : /voguemind/processed/transactions/  (Parquet, partitionné par t_dat)
Rapport : /voguemind/processed/quality/transactions/  (une ligne par exécution)

Règles :
  - doublons : un même transaction_id envoyé plusieurs fois (producer relancé)
    -> on garde la version la plus récemment ingérée ;
  - valeurs hors bornes : prix <= 0 ou > 1 (les prix H&M sont normalisés entre 0 et 1).

Lancement :
    docker compose exec spark-master /opt/spark/bin/spark-submit /opt/voguemind/batch/clean_transactions.py
"""
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BASE = os.environ.get("VOGUEMIND_HDFS_BASE", "hdfs://namenode:9000/voguemind")
RAW = f"{BASE}/raw/transactions"
OUT = f"{BASE}/processed/transactions"
QUALITY = f"{BASE}/processed/quality/transactions"


def main() -> int:
    spark = SparkSession.builder.appName("voguemind-clean-transactions").getOrCreate()
    jvm = spark.sparkContext._jvm
    raw_path = jvm.org.apache.hadoop.fs.Path(RAW)
    if not raw_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).exists(raw_path):
        print("Aucune donnée dans", RAW, "— lancer d'abord le producer puis streaming/kafka_to_hdfs.py")
        return 1
    raw = spark.read.parquet(RAW)
    n_raw = raw.count()
    if n_raw == 0:
        print("Aucune transaction dans", RAW, "— lancer d'abord le producer puis streaming/kafka_to_hdfs.py")
        return 1

    latest_first = Window.partitionBy("transaction_id").orderBy(F.col("ingestion_ts").desc_nulls_last(),
                                                                F.col("kafka_offset").desc())
    dedup = (raw.withColumn("_rank", F.row_number().over(latest_first))
             .filter("_rank = 1").drop("_rank"))
    n_dedup = dedup.count()

    clean = (dedup.filter((F.col("price") > 0) & (F.col("price") <= 1))
             .select("transaction_id", "customer_id", "article_id", "t_dat", "price",
                     "sales_channel_id", "ingestion_ts"))
    clean.write.mode("overwrite").partitionBy("t_dat").parquet(OUT)
    n_clean = spark.read.parquet(OUT).count()

    report = spark.createDataFrame(
        [(n_raw, n_raw - n_dedup, n_dedup - n_clean, n_clean)],
        "lignes_brutes long, doublons_supprimes long, hors_bornes long, lignes_propres long",
    ).withColumn("run_ts", F.current_timestamp())
    report.write.mode("append").parquet(QUALITY)

    print("\n=== Qualité des transactions ===")
    report.show(truncate=False)
    print("Valeurs manquantes par colonne (après nettoyage) :")
    spark.read.parquet(OUT).select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in
         ["customer_id", "article_id", "t_dat", "price", "sales_channel_id"]]).show()
    print(f"Sortie : {OUT}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
