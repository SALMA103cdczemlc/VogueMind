"""
VogueMind — Export des indicateurs HDFS vers la base de restitution PostgreSQL.

Chaque dossier /voguemind/processed/indicators/<nom>/ devient une table ind_<nom>
(recréée à chaque exécution). Les tables sont petites : elles passent par le driver.

Lancement :
    docker compose exec spark-master /opt/spark/bin/spark-submit /opt/voguemind/batch/export_to_postgres.py
"""
import os
import sys

from pyspark.sql import SparkSession

BASE = os.environ.get("VOGUEMIND_HDFS_BASE", "hdfs://namenode:9000/voguemind")
PG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "voguemind"),
    "user": os.environ.get("POSTGRES_USER", "voguemind"),
    "password": os.environ.get("POSTGRES_PASSWORD", "voguemind"),
}
SQL_TYPES = {"string": "TEXT", "int": "INTEGER", "bigint": "BIGINT", "double": "DOUBLE PRECISION",
             "date": "DATE", "timestamp": "TIMESTAMPTZ", "float": "REAL", "boolean": "BOOLEAN"}


def main() -> int:
    import psycopg2
    from psycopg2.extras import execute_values

    spark = SparkSession.builder.appName("voguemind-export-postgres").getOrCreate()
    root = f"{BASE}/processed/indicators"
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(root)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not fs.exists(path):
        print("ERREUR :", root, "absent — lancer d'abord batch/indicators.py")
        return 1
    names = sorted(s.getPath().getName() for s in fs.listStatus(path) if s.isDirectory())

    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        for name in names:
            df = spark.read.parquet(f"{root}/{name}")
            cols = [(f.name, SQL_TYPES.get(f.dataType.simpleString(), "TEXT")) for f in df.schema.fields]
            table = f"ind_{name}"
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(f"CREATE TABLE {table} ({', '.join(f'{c} {t}' for c, t in cols)})")
            rows = [tuple(r) for r in df.collect()]
            if rows:
                execute_values(cur, f"INSERT INTO {table} ({', '.join(c for c, _ in cols)}) VALUES %s", rows)
            print(f"{table:<28} {len(rows):>6} lignes")
    print(f"\nIndicateurs exportés vers PostgreSQL ({PG['host']}:{PG['port']}/{PG['dbname']})")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
