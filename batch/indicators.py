"""
VogueMind — Indicateurs métier (analyse batch, Spark SQL) sur l'historique HDFS.

Entrées : /voguemind/processed/transactions/  (+ articles, + clients si disponibles)
Sorties : /voguemind/processed/indicators/<indicateur>/  (Parquet)

  kpi_global          volume, clients, articles, chiffre d'affaires, panier moyen, période
  ventes_par_jour     évolution temporelle des ventes
  top_articles        20 articles les plus vendus (avec nom et type de produit)
  ventes_par_categorie  ventes et part du CA par groupe de produits
  ventes_par_rayon    ventes par rayon (Ladieswear, Menswear, ...)
  ventes_par_canal    répartition par canal de vente (sales_channel_id)
  ventes_par_age      ventes par tranche d'âge (si customers disponible)

Les prix H&M sont normalisés par Kaggle : le « chiffre d'affaires » est exprimé
dans cette unité normalisée (comparaisons relatives uniquement).

Lancement :
    docker compose exec spark-master /opt/spark/bin/spark-submit /opt/voguemind/batch/indicators.py
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = os.environ.get("VOGUEMIND_HDFS_BASE", "hdfs://namenode:9000/voguemind")
OUT = f"{BASE}/processed/indicators"


def exists(spark, p):
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(p)
    return path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).exists(path)


def save(df, name, show=10):
    df.write.mode("overwrite").parquet(f"{OUT}/{name}")
    print(f"\n=== {name} ===  ->  {OUT}/{name}")
    df.show(show, truncate=40)


def main() -> int:
    spark = SparkSession.builder.appName("voguemind-indicators").getOrCreate()
    tx_path = f"{BASE}/processed/transactions"
    if not exists(spark, tx_path):
        print("ERREUR :", tx_path, "absent — lancer d'abord batch/clean_transactions.py")
        return 1
    tx = spark.read.parquet(tx_path).cache()
    tx.createOrReplaceTempView("transactions")

    has_articles = exists(spark, f"{BASE}/processed/articles")
    has_customers = exists(spark, f"{BASE}/processed/customers")
    if has_articles:
        spark.read.parquet(f"{BASE}/processed/articles").createOrReplaceTempView("articles")
    if has_customers:
        spark.read.parquet(f"{BASE}/processed/customers").createOrReplaceTempView("customers")

    # 1. KPI globaux
    save(spark.sql("""
        SELECT COUNT(*)                         AS nb_transactions,
               COUNT(DISTINCT customer_id)      AS nb_clients,
               COUNT(DISTINCT article_id)       AS nb_articles,
               ROUND(SUM(price), 4)             AS chiffre_affaires,
               ROUND(SUM(price) / COUNT(DISTINCT customer_id, t_dat), 5) AS panier_moyen,
               ROUND(COUNT(*) / COUNT(DISTINCT customer_id, t_dat), 2)   AS articles_par_panier,
               MIN(t_dat) AS premiere_date, MAX(t_dat) AS derniere_date
        FROM transactions"""), "kpi_global")

    # 2. Évolution temporelle
    save(spark.sql("""
        SELECT t_dat, COUNT(*) AS nb_ventes, COUNT(DISTINCT customer_id) AS nb_clients,
               ROUND(SUM(price), 4) AS chiffre_affaires
        FROM transactions GROUP BY t_dat ORDER BY t_dat"""), "ventes_par_jour")

    # 3. Canal de vente
    save(spark.sql("""
        SELECT sales_channel_id, COUNT(*) AS nb_ventes, ROUND(SUM(price), 4) AS chiffre_affaires,
               ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS part_ventes_pct
        FROM transactions GROUP BY sales_channel_id ORDER BY sales_channel_id"""), "ventes_par_canal")

    if has_articles:
        # Couverture de la jointure : part des transactions dont l'article est connu
        cov = spark.sql("""
            SELECT ROUND(100 * AVG(CASE WHEN a.article_id IS NULL THEN 0 ELSE 1 END), 2) AS pct
            FROM transactions t LEFT JOIN articles a ON t.article_id = a.article_id""").first()["pct"]
        print(f"\nCouverture articles : {cov} % des transactions ont un article référencé")

        # 4. Top articles
        save(spark.sql("""
            SELECT t.article_id, a.prod_name,
                   a.product_type_name, a.colour_group_name,
                   COUNT(*) AS nb_ventes, ROUND(SUM(t.price), 4) AS chiffre_affaires
            FROM transactions t JOIN articles a ON t.article_id = a.article_id
            GROUP BY t.article_id, a.prod_name, a.product_type_name, a.colour_group_name
            ORDER BY nb_ventes DESC, chiffre_affaires DESC LIMIT 20"""), "top_articles")

        # 5. Catégories et rayons
        for name, col in [("ventes_par_categorie", "product_group_name"),
                          ("ventes_par_rayon", "index_group_name")]:
            save(spark.sql(f"""
                SELECT COALESCE(a.{col}, '(inconnu)') AS {col},
                       COUNT(*) AS nb_ventes, ROUND(SUM(t.price), 4) AS chiffre_affaires,
                       ROUND(100 * SUM(t.price) / SUM(SUM(t.price)) OVER (), 2) AS part_ca_pct
                FROM transactions t LEFT JOIN articles a ON t.article_id = a.article_id
                GROUP BY COALESCE(a.{col}, '(inconnu)') ORDER BY nb_ventes DESC"""), name)
    else:
        print("\n[info] processed/articles absent : indicateurs par article/catégorie ignorés "
              "(lancer batch/load_reference.py)")

    if has_customers:
        # 6. Tranches d'âge
        save(spark.sql("""
            SELECT COALESCE(c.age_band, 'inconnu') AS tranche_age, COUNT(*) AS nb_ventes,
                   COUNT(DISTINCT t.customer_id) AS nb_clients, ROUND(SUM(t.price), 4) AS chiffre_affaires
            FROM transactions t LEFT JOIN customers c ON t.customer_id = c.customer_id
            GROUP BY COALESCE(c.age_band, 'inconnu') ORDER BY tranche_age"""), "ventes_par_age")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
