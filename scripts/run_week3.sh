#!/usr/bin/env bash
# =============================================================================
#  VogueMind — Semaine 3 : historique -> indicateurs -> modèle ML (réachat 7 jours)
#  Prérequis : data/transactions_train.csv (+ articles.csv, customers.csv conseillés)
#  Usage : ./scripts/run_week3.sh            (paramètres modifiables ci-dessous)
#          START=2018-09-20 WEEKS=12 PCT=5 VERSION=v1 ./scripts/run_week3.sh
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
START=${START:-2018-09-20}   # début de l'historique
WEEKS=${WEEKS:-12}           # durée de l'historique (semaines)
PCT=${PCT:-5}                # % de clients utilisés pour l'entraînement
VERSION=${VERSION:-v1}       # version du modèle sauvegardé

SUBMIT="docker compose exec -T spark-master /opt/spark/bin/spark-submit"
APP=/opt/voguemind
title() { echo; echo "=================================================="; echo " $1"; echo "=================================================="; }

title "0. Vérifications"
if [ ! -f data/transactions_train.csv ]; then
  echo "data/transactions_train.csv absent (sauf s'il est déjà dans HDFS /voguemind/raw/)."
fi
for v in infra_namenode_data infra_datanode1_data infra_datanode2_data; do
  docker volume inspect "$v" >/dev/null 2>&1 || { echo "Création du volume $v"; docker volume create "$v" >/dev/null; }
done
docker compose up -d --build
echo "Attente du cluster..."; sleep 20

title "1. Historique des transactions -> HDFS"
$SUBMIT $APP/batch/load_history.py --start-date "$START" --weeks "$WEEKS" 2>/dev/null

title "2. Référentiels articles / clients -> HDFS"
$SUBMIT $APP/batch/load_reference.py 2>/dev/null

title "3. Indicateurs métier sur l'historique (Spark SQL)"
$SUBMIT $APP/batch/indicators.py --source history 2>/dev/null

title "4. Entraînement : référence + 3 techniques + réglage + test"
$SUBMIT $APP/batch/train_model.py --customer-pct "$PCT" --version "$VERSION" 2>/dev/null

title "5. Export des indicateurs vers PostgreSQL"
$SUBMIT $APP/batch/export_to_postgres.py 2>/dev/null

title "6. Modèle sauvegardé dans HDFS"
docker compose exec -T namenode hdfs dfs -ls /voguemind/models
echo; echo "Rapport : results/ml_metrics_${VERSION}.md  ·  Tableau de bord : http://localhost:8501"
