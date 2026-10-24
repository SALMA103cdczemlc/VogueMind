"""
Test d'intégration Spark <-> HDFS : écriture Parquet distribuée par les
workers dans HDFS, puis relecture et contrôle du contenu.

Usage :
    docker compose exec spark-master spark-submit \
        /opt/voguemind/tests/integration/test_hdfs_integration.py

Variable optionnelle : VOGUEMIND_HDFS_BASE (défaut hdfs://namenode:9000/voguemind)
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = os.environ.get("VOGUEMIND_HDFS_BASE", "hdfs://namenode:9000/voguemind")
TARGET = f"{BASE}/tests/spark_hdfs_check"
ROWS = 100_000


def main() -> int:
    spark = SparkSession.builder.appName("voguemind-test-hdfs").getOrCreate()
    results = {}
    print("\n=== Test Spark <-> HDFS ===")
    print(f"Cible : {TARGET}")

    # Données factices au format des transactions H&M
    df = (spark.range(ROWS)
          .withColumn("customer_id", F.concat(F.lit("c_"), (F.col("id") % 5000).cast("string")))
          .withColumn("article_id", (F.col("id") % 800 + 100000000).cast("long"))
          .withColumn("price", F.round(F.rand(seed=42) * 0.1, 4))
          .withColumn("t_dat", F.date_add(F.lit("2020-09-01").cast("date"), (F.col("id") % 22).cast("int")))
          .drop("id"))

    df.repartition(4).write.mode("overwrite").partitionBy("t_dat").parquet(TARGET)
    print("Écriture Parquet terminée")

    back = spark.read.parquet(TARGET)
    n = back.count()
    results[f"Relecture de {ROWS} lignes"] = n == ROWS
    results["Partitionnement par date (22 jours)"] = back.select("t_dat").distinct().count() == 22
    results["Schéma conservé"] = set(back.columns) == {"customer_id", "article_id", "price", "t_dat"}

    # Réplication effective des fichiers écrits (API Hadoop via la JVM)
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(TARGET)
    fs = path.getFileSystem(conf)
    files = fs.listFiles(path, True)
    replications = set()
    while files.hasNext():
        st = files.next()
        if st.getPath().getName().endswith(".parquet"):
            replications.add(st.getReplication())
    print(f"Facteur(s) de réplication observé(s) : {sorted(replications)}")
    if fs.getScheme() == "hdfs":
        results["Réplication HDFS = 2"] = replications == {2}

    print("\n--- Résultats ---")
    for name, ok in results.items():
        print(f"[{'OK' if ok else 'ECHEC'}] {name}")
    spark.stop()
    ok = all(results.values())
    print("RESULTAT :", "intégration Spark-HDFS opérationnelle" if ok else "au moins un test a échoué")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
