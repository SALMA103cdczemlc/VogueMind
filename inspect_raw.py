from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("inspect-voguemind-raw").getOrCreate()

path = "hdfs://namenode:9000/voguemind/raw/transactions"
df = spark.read.parquet(path)

print("\n=== SCHEMA ===")
df.printSchema()

print("\n=== COLONNES ===")
print(df.columns)

print("\n=== NOMBRE DE LIGNES ===")
print(df.count())

print("\n=== EXEMPLE DE 5 TRANSACTIONS ===")
df.show(5, truncate=False)

print("\n=== VERIFICATION DES COLONNES ATTENDUES ===")
for col in ["transaction_id", "ingestion_ts", "kafka_offset",
            "customer_id", "article_id", "t_dat", "price",
            "sales_channel_id"]:
    print(f"{col}: {'OK' if col in df.columns else 'ABSENTE'}")

spark.stop()
