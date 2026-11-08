"""
VogueMind — Chargement des données de référence H&M (articles, clients) en Parquet.

Pour chaque fichier, la première source trouvée est utilisée :
  articles : hdfs /voguemind/raw/articles.csv  ->  data/articles.csv (dossier local monté)
             ->  hdfs /voguemind/raw/articles_sample.csv
  clients  : hdfs /voguemind/raw/customers.csv ->  data/customers.csv  (facultatif)

Sorties : /voguemind/processed/articles/  et  /voguemind/processed/customers/

Lancement :
    docker compose exec spark-master /opt/spark/bin/spark-submit /opt/voguemind/batch/load_reference.py
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = os.environ.get("VOGUEMIND_HDFS_BASE", "hdfs://namenode:9000/voguemind")
LOCAL_DATA = os.environ.get("VOGUEMIND_LOCAL_DATA", "file:///opt/voguemind/data")

SOURCES = {
    "articles": [f"{BASE}/raw/articles.csv", f"{LOCAL_DATA}/articles.csv", f"{BASE}/raw/articles_sample.csv"],
    "customers": [f"{BASE}/raw/customers.csv", f"{LOCAL_DATA}/customers.csv"],
}

ARTICLE_COLUMNS = ["article_id", "product_code", "prod_name", "product_type_name", "product_group_name",
                   "graphical_appearance_name", "colour_group_name", "perceived_colour_master_name",
                   "department_name", "index_name", "index_group_name", "section_name",
                   "garment_group_name", "detail_desc"]


def first_existing(spark, paths):
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    for p in paths:
        path = jvm.org.apache.hadoop.fs.Path(p)
        if path.getFileSystem(conf).exists(path):
            return p
    return None


def read_csv(spark, path):
    # Tout en texte : préserve les zéros initiaux des identifiants (article_id = 0108775015)
    return spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(path)


def main() -> int:
    spark = SparkSession.builder.appName("voguemind-load-reference").getOrCreate()

    # --- Articles (obligatoire)
    src = first_existing(spark, SOURCES["articles"])
    if src is None:
        print("ERREUR : aucun fichier articles trouvé. Mettre articles.csv dans data/ ou dans HDFS /voguemind/raw/")
        return 1
    articles = (read_csv(spark, src)
                .select(*ARTICLE_COLUMNS)
                .withColumn("article_id", F.trim("article_id"))
                .filter(F.col("article_id").rlike("^[0-9]{9,10}$"))
                .withColumn("article_id", F.lpad("article_id", 10, "0"))
                .dropDuplicates(["article_id"]))
    articles.write.mode("overwrite").parquet(f"{BASE}/processed/articles")
    n_art = spark.read.parquet(f"{BASE}/processed/articles").count()
    print(f"[articles]  source : {src}")
    print(f"[articles]  {n_art} articles -> {BASE}/processed/articles")
    if "sample" in src:
        print("[articles]  ATTENTION : échantillon seulement — les jointures avec les transactions "
              "seront partielles. Ajouter le fichier complet articles.csv.")

    # --- Clients (facultatif)
    src = first_existing(spark, SOURCES["customers"])
    if src is None:
        print("[customers] aucun fichier customers.csv : étape ignorée")
    else:
        customers = (read_csv(spark, src)
                     .select("customer_id",
                             F.col("age").cast("int").alias("age"),
                             "club_member_status", "fashion_news_frequency",
                             F.col("Active").cast("double").alias("active"),
                             F.col("FN").cast("double").alias("fn"))
                     .dropDuplicates(["customer_id"])
                     .withColumn("age_band",
                                 F.when(F.col("age").isNull(), "inconnu")
                                 .when(F.col("age") < 25, "16-24")
                                 .when(F.col("age") < 35, "25-34")
                                 .when(F.col("age") < 45, "35-44")
                                 .when(F.col("age") < 55, "45-54")
                                 .otherwise("55+")))
        customers.write.mode("overwrite").parquet(f"{BASE}/processed/customers")
        print(f"[customers] source : {src}")
        print(f"[customers] {spark.read.parquet(f'{BASE}/processed/customers').count()} clients "
              f"-> {BASE}/processed/customers")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
