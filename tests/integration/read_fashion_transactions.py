"""
Lecture par Spark (sur le cluster) des vraies transactions H&M publiées par
ingestion/producer.py dans le topic `fashion-transactions`.

Valide la chaîne réelle : producer.py (hôte) -> Kafka -> Spark (conteneurs).

Usage (après avoir lancé `python ingestion/producer.py` sur la machine hôte) :
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        /opt/voguemind/tests/integration/read_fashion_transactions.py
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = "fashion-transactions"

SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("article_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("price", DoubleType()),
    StructField("sales_channel_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("ingestion_timestamp", StringType()),
])


def main() -> int:
    spark = SparkSession.builder.appName("voguemind-read-fashion-transactions").getOrCreate()
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .load())
    events = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"),
                         "partition", "offset")
              .select("e.*", "partition", "offset"))

    total = events.count()
    print(f"\n=== Topic {TOPIC} lu par Spark : {total} événements ===")
    if total == 0:
        print("ECHEC : topic vide — lancer d'abord `python ingestion/producer.py`")
        spark.stop()
        return 1

    events.groupBy("partition").count().orderBy("partition").show()
    events.select("transaction_id", "customer_id", "article_id", "timestamp", "price",
                  "sales_channel_id").show(5, truncate=20)
    (events.agg(F.countDistinct("customer_id").alias("clients"),
                F.countDistinct("article_id").alias("articles"),
                F.round(F.avg("price"), 5).alias("prix_moyen"),
                F.min("timestamp").alias("premiere_date"),
                F.max("timestamp").alias("derniere_date"))
     .show())
    invalid = events.filter(F.col("customer_id").isNull() | F.col("price").isNull()).count()
    print(f"Messages invalides (JSON non conforme) : {invalid}")
    print("OK chaîne producer.py -> Kafka -> Spark opérationnelle")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
