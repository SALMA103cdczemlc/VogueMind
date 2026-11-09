
"""
VogueMind - Validation du stockage du pipeline Kafka -> Spark -> HDFS.

Controle les transactions Parquet deja archivees et la replication HDFS.
Ne publie pas de nouveaux messages Kafka et ne modifie pas HDFS.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PATH = "hdfs://namenode:9000/voguemind/raw/transactions"


def main():
    spark = SparkSession.builder.appName(
        "VogueMind-End-to-End-Validation"
    ).getOrCreate()

    checks = {}

    try:
        df = spark.read.parquet(PATH)
        total = df.count()

        checks["Transactions archivees"] = total > 0

        required = {
            "transaction_id", "customer_id", "article_id",
            "t_dat", "price", "sales_channel_id"
        }
        checks["Schema attendu"] = required.issubset(set(df.columns))

        if checks["Schema attendu"]:
            invalid = df.filter(
                F.col("customer_id").isNull()
                | F.col("article_id").isNull()
                | F.col("t_dat").isNull()
                | F.col("price").isNull()
                | (F.col("price") <= 0)
                | (~F.col("sales_channel_id").isin(1, 2))
            ).count()
            checks["Qualite des transactions"] = invalid == 0

        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        path = jvm.org.apache.hadoop.fs.Path(PATH)
        fs = path.getFileSystem(conf)

        files = fs.listFiles(path, True)
        replications = []

        while files.hasNext():
            file = files.next()
            if file.getPath().getName().endswith(".parquet"):
                replications.append(file.getReplication())

        checks["Fichiers Parquet presents"] = len(replications) > 0
        checks["Replication HDFS = 2"] = (
            len(replications) > 0
            and all(r == 2 for r in replications)
        )

        print("\n=== VOGUEMIND - VALIDATION HDFS ===")
        print(f"Transactions archivees : {total}")
        print(f"Fichiers Parquet : {len(replications)}")

        for name, passed in checks.items():
            print(f"[{'PASS' if passed else 'FAIL'}] {name}")

        success = bool(checks) and all(checks.values())
        print("RESULTAT :", "SUCCES" if success else "ECHEC")
        return 0 if success else 1

    except Exception as exc:
        print(f"[FAIL] Erreur de validation : {exc}")
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
