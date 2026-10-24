"""
Test d'intégration Kafka -> Spark (batch + Structured Streaming) -> HDFS.

1. Spark publie N événements de test dans le topic Kafka `integration-tests`
2. Spark relit le topic en batch et retrouve les N événements
3. Un job Structured Streaming consomme le topic et écrit dans HDFS
   (avec checkpoint), puis on vérifie le contenu écrit.

C'est la chaîne qui sera utilisée par la voie chaude du projet.

Usage :
    docker compose exec spark-master spark-submit \
        /opt/voguemind/tests/integration/test_kafka_integration.py
"""
import os
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.environ.get("KAFKA_TEST_TOPIC", "integration-tests")
BASE = os.environ.get("VOGUEMIND_HDFS_BASE", "hdfs://namenode:9000/voguemind")
N = 500

# Même format que les événements de ingestion/producer.py (+ run_id pour isoler le test)
EVENT_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("article_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("price", DoubleType()),
    StructField("sales_channel_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("ingestion_timestamp", StringType()),
    StructField("run_id", StringType()),
])


def main() -> int:
    spark = SparkSession.builder.appName("voguemind-test-kafka").getOrCreate()
    run_id = uuid.uuid4().hex[:8]
    out_path = f"{BASE}/tests/stream_{run_id}"
    ckpt_path = f"{BASE}/checkpoints/test_{run_id}"
    results = {}

    print("\n=== Test Kafka -> Spark -> HDFS ===")
    print(f"Broker : {BOOTSTRAP} | topic : {TOPIC} | run_id : {run_id}")

    # 1) Production de N événements (format proche des transactions H&M)
    events = (spark.range(N)
              .select(
                  F.concat(F.lit(f"tx_{run_id}_"), F.col("id").cast("string")).alias("transaction_id"),
                  F.concat(F.lit("c_"), (F.col("id") % 50).cast("string")).alias("customer_id"),
                  F.lpad((F.col("id") % 30 + 108775015).cast("string"), 10, "0").alias("article_id"),
                  F.lit("2020-09-22").alias("timestamp"),
                  F.round(F.lit(0.0169) + (F.col("id") % 7) / 1000, 4).alias("price"),
                  (F.col("id") % 2 + 1).cast("int").alias("sales_channel_id"),
                  F.lit("purchase").alias("event_type"),
                  F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("ingestion_timestamp"),
                  F.lit(run_id).alias("run_id")))
    (events.select(F.col("customer_id").alias("key"),
                   F.to_json(F.struct(*events.columns)).alias("value"))
     .write.format("kafka")
     .option("kafka.bootstrap.servers", BOOTSTRAP)
     .option("topic", TOPIC)
     .save())
    print(f"{N} événements publiés dans Kafka")

    # 2) Lecture batch du topic
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .load())
    parsed = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")).select("e.*")
    got = parsed.filter(F.col("run_id") == run_id).count()
    results["Lecture batch Kafka : N événements retrouvés"] = got == N

    # 3) Structured Streaming : Kafka -> HDFS (Parquet + checkpoint)
    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", TOPIC)
              .option("startingOffsets", "earliest")
              .load()
              .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
                      F.col("timestamp").alias("kafka_timestamp"))
              .select("e.*", "kafka_timestamp")
              .filter(F.col("run_id") == run_id))
    query = (stream.writeStream.format("parquet")
             .option("path", out_path)
             .option("checkpointLocation", ckpt_path)
             .trigger(availableNow=True)
             .start())
    query.awaitTermination()
    written = spark.read.parquet(out_path)
    results["Streaming Kafka -> HDFS : N lignes écrites"] = written.count() == N
    results["Prix lus correctement"] = written.filter(F.col("price").isNull()).count() == 0

    # 4) Reprise : relancer le même stream ne doit rien réécrire (checkpoint)
    q2 = (stream.writeStream.format("parquet")
          .option("path", out_path)
          .option("checkpointLocation", ckpt_path)
          .trigger(availableNow=True)
          .start())
    q2.awaitTermination()
    results["Reprise sur checkpoint (pas de doublons)"] = spark.read.parquet(out_path).count() == N

    print(f"Sortie streaming : {out_path}")
    print("\n--- Résultats ---")
    for name, ok in results.items():
        print(f"[{'OK' if ok else 'ECHEC'}] {name}")
    spark.stop()
    ok = all(results.values())
    print("RESULTAT :", "chaîne Kafka -> Spark -> HDFS opérationnelle" if ok else "au moins un test a échoué")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
