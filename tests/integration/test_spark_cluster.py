"""
Test du cluster Spark : vérifie que le job est réellement distribué
sur les deux workers et que le calcul est correct.

Usage :
    docker compose exec spark-master spark-submit \
        /opt/voguemind/tests/integration/test_spark_cluster.py
"""
import socket
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

EXPECTED_WORKERS = 2
N = 2_000_000
PARTITIONS = 8


def host_of_partition(_):
    yield socket.gethostname()


def main() -> int:
    spark = SparkSession.builder.appName("voguemind-test-spark-cluster").getOrCreate()
    sc = spark.sparkContext
    results = {}

    print("\n=== Test du cluster Spark ===")
    print(f"Master       : {sc.master}")
    print(f"Version      : {spark.version}")
    print(f"Driver       : {socket.gethostname()}")

    # Attendre que les executors s'enregistrent (jusqu'à 30 s)
    deadline = time.time() + 30
    executor_hosts = []
    while time.time() < deadline:
        infos = sc._jsc.sc().statusTracker().getExecutorInfos()  # inclut le driver
        executor_hosts = sorted(f"{i.host()}:{i.port()}" for i in infos)
        if len(executor_hosts) - 1 >= EXPECTED_WORKERS:
            break
        time.sleep(2)
    print(f"JVM actives : {executor_hosts} (driver inclus)")

    # 1) Répartition : sur quels conteneurs les tâches s'exécutent-elles ?
    hosts = sorted(set(sc.parallelize(range(PARTITIONS * 4), PARTITIONS * 4)
                       .mapPartitions(host_of_partition).collect()))
    print(f"Tâches exécutées sur : {hosts}")
    results["Tâches réparties sur >= 2 workers"] = len(hosts) >= EXPECTED_WORKERS

    # 2) Calcul RDD distribué
    total = sc.parallelize(range(1, N + 1), PARTITIONS).sum()
    results["Somme RDD correcte"] = total == N * (N + 1) // 2

    # 3) Spark SQL : agrégation avec shuffle
    df = spark.range(0, N, numPartitions=PARTITIONS).withColumn("bucket", F.col("id") % 10)
    agg = df.groupBy("bucket").count().orderBy("bucket").collect()
    results["Agrégation Spark SQL (shuffle)"] = len(agg) == 10 and all(r["count"] == N // 10 for r in agg)

    # 4) UDF Python : prouve que Python fonctionne sur les executors
    plus_one = F.udf(lambda x: x + 1, "long")
    s = df.limit(1000).select(plus_one("id").alias("v")).agg(F.sum("v")).first()[0]
    results["UDF Python sur les executors"] = s == sum(range(1, 1001))

    print("\n--- Résultats ---")
    for name, ok in results.items():
        print(f"[{'OK' if ok else 'ECHEC'}] {name}")
    spark.stop()
    ok = all(results.values())
    print("RESULTAT :", "cluster Spark opérationnel" if ok else "au moins un test a échoué")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
