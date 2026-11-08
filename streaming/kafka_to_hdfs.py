"""
VogueMind — Voie froide : Kafka -> Spark Structured Streaming -> HDFS

Lit en continu les transactions H&M publiées par ingestion/producer.py dans le
topic Kafka `fashion-transactions`, valide chaque message, puis archive :
  - les transactions valides   -> /voguemind/raw/transactions/            (Parquet, partitionné par t_dat)
  - les messages rejetés       -> /voguemind/raw/transactions_rejected/   (Parquet, avec le motif du rejet)
Le checkpoint (/voguemind/checkpoints/kafka_to_hdfs/) permet de reprendre
exactement là où le job s'est arrêté, sans perte ni relecture.

Lancement (depuis la racine du dépôt) :
    # en continu (Ctrl+C pour arrêter)
    docker compose exec spark-master /opt/spark/bin/spark-submit /opt/voguemind/streaming/kafka_to_hdfs.py
    # une seule passe : traite tout ce qui est disponible puis s'arrête (démo, tests)
    docker compose exec spark-master /opt/spark/bin/spark-submit /opt/voguemind/streaming/kafka_to_hdfs.py --once
"""
import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

BASE = os.environ.get("VOGUEMIND_HDFS_BASE", "hdfs://namenode:9000/voguemind")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.environ.get("KAFKA_TOPIC", "fashion-transactions")

RAW_PATH = f"{BASE}/raw/transactions"
REJECTED_PATH = f"{BASE}/raw/transactions_rejected"
CHECKPOINT = f"{BASE}/checkpoints/kafka_to_hdfs"

# Format des événements produits par ingestion/producer.py
EVENT_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("article_id", StringType()),
    StructField("timestamp", StringType()),          # date d'achat H&M (t_dat), ex. 2018-09-20
    StructField("price", DoubleType()),
    StructField("sales_channel_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("ingestion_timestamp", StringType()),
])


def parse_and_validate(kafka_df: DataFrame) -> DataFrame:
    """Message Kafka brut -> une ligne typée + colonne `reject_reason` (null si valide)."""
    parsed = (kafka_df
              .select(F.col("value").cast("string").alias("raw_value"),
                      F.col("partition").alias("kafka_partition"),
                      F.col("offset").alias("kafka_offset"),
                      F.col("timestamp").alias("kafka_timestamp"))
              .withColumn("e", F.from_json("raw_value", EVENT_SCHEMA))
              .select("raw_value", "kafka_partition", "kafka_offset", "kafka_timestamp", "e.*"))

    typed = (parsed
             .withColumn("t_dat", F.to_date("timestamp", "yyyy-MM-dd"))
             .withColumn("ingestion_ts", F.to_timestamp("ingestion_timestamp"))
             # article_id H&M = 10 chiffres avec zéro initial (ex. 0663713001) ;
             # on rétablit le zéro s'il a été perdu (9 chiffres), sinon on garde la valeur telle quelle
             .withColumn("article_id", F.when(F.trim("article_id").rlike("^[0-9]{9}$"),
                                              F.lpad(F.trim("article_id"), 10, "0"))
                         .otherwise(F.trim("article_id")))
             .withColumn("customer_id", F.trim("customer_id")))

    reason = (F.when(F.col("transaction_id").isNull() & F.col("customer_id").isNull(), "json_invalide")
              .when(F.col("customer_id").isNull() | (F.length("customer_id") == 0), "customer_id_manquant")
              .when(F.col("article_id").isNull() | ~F.col("article_id").rlike("^[0-9]{10}$"), "article_id_invalide")
              .when(F.col("t_dat").isNull(), "date_invalide")
              .when(F.col("price").isNull() | (F.col("price") <= 0), "prix_invalide")
              .when(~F.col("sales_channel_id").isin(1, 2), "canal_invalide"))
    return typed.withColumn("reject_reason", reason)


VALID_COLUMNS = ["transaction_id", "customer_id", "article_id", "t_dat", "price",
                 "sales_channel_id", "event_type", "ingestion_ts",
                 "kafka_partition", "kafka_offset", "kafka_timestamp"]


def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    batch_df.persist()
    valid = batch_df.filter(F.col("reject_reason").isNull()).select(*VALID_COLUMNS)
    rejected = (batch_df.filter(F.col("reject_reason").isNotNull())
                .select("reject_reason", "raw_value", "kafka_partition", "kafka_offset", "kafka_timestamp"))
    n_valid, n_rejected = valid.count(), rejected.count()
    if n_valid:
        valid.write.mode("append").partitionBy("t_dat").parquet(RAW_PATH)
    if n_rejected:
        rejected.write.mode("append").parquet(REJECTED_PATH)
    print(f"[batch {batch_id}] {n_valid} transactions archivées, {n_rejected} rejetées", flush=True)
    batch_df.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true",
                        help="traite tous les messages disponibles puis s'arrête")
    parser.add_argument("--interval", default="10 seconds", help="intervalle entre micro-batchs")
    parser.add_argument("--max-offsets", type=int, default=20000,
                        help="nombre max de messages Kafka par micro-batch")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("voguemind-kafka-to-hdfs").getOrCreate()
    print(f"Kafka {BOOTSTRAP} / topic {TOPIC}  ->  {RAW_PATH}")
    print(f"Checkpoint : {CHECKPOINT}")

    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", TOPIC)
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", args.max_offsets)
              .option("failOnDataLoss", "false")
              .load())

    writer = (parse_and_validate(source).writeStream
              .foreachBatch(write_batch)
              .option("checkpointLocation", CHECKPOINT)
              .queryName("kafka_to_hdfs"))
    writer = writer.trigger(availableNow=True) if args.once else writer.trigger(processingTime=args.interval)

    query = writer.start()
    query.awaitTermination()
    print("Job terminé.")


if __name__ == "__main__":
    main()
