#!/usr/bin/env bash
# =============================================================================
#  VogueMind — Démonstration semaine 2 : Kafka -> Spark -> HDFS -> analyse batch
#  Prérequis : cluster lancé (docker compose up -d) et transactions publiées
#  dans Kafka par ingestion/producer.py
#  Usage (racine du dépôt) : ./scripts/run_week2.sh
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
SUBMIT="docker compose exec -T spark-master /opt/spark/bin/spark-submit"
APP=/opt/voguemind
HDFS="docker compose exec -T namenode hdfs"

title() { echo; echo "=================================================="; echo " $1"; echo "=================================================="; }

title "1. Messages disponibles dans Kafka (fashion-transactions)"
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:29092 --topic fashion-transactions || true

title "2. Streaming Kafka -> HDFS (une passe : --once)"
$SUBMIT $APP/streaming/kafka_to_hdfs.py --once 2>/dev/null

title "3. Données de référence (articles, clients) -> Parquet"
$SUBMIT $APP/batch/load_reference.py 2>/dev/null

title "4. Nettoyage de l'historique (doublons, valeurs hors bornes)"
$SUBMIT $APP/batch/clean_transactions.py 2>/dev/null

title "5. Indicateurs métier (Spark SQL)"
$SUBMIT $APP/batch/indicators.py 2>/dev/null

title "6. Fichiers produits dans HDFS"
$HDFS dfs -du -h /voguemind/raw /voguemind/processed /voguemind/processed/indicators
$HDFS fsck /voguemind/processed -files -blocks 2>/dev/null | grep -E "Total files|Average block replication|Under-replicated|HEALTHY|CORRUPT"
echo; echo "Démonstration semaine 2 terminée."
