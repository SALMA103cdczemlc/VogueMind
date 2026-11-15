#!/usr/bin/env bash
# =============================================================================
#  VogueMind — Semaine 4 : inférence temps réel + base de restitution + tableau de bord
#  Prérequis : ./scripts/run_week3.sh exécuté (modèle sauvegardé dans HDFS)
#  Usage : ./scripts/run_week4.sh          démarre le job de prédiction en arrière-plan
#          ./scripts/run_week4.sh status   affiche le journal et le contenu de la base
#          ./scripts/run_week4.sh stop     arrête le job (le checkpoint permet de reprendre)
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
VERSION=${VERSION:-v1}
LOG=/tmp/predict_stream.log
PSQL="docker compose exec -T postgres psql -U voguemind -d voguemind"

status() {
  echo "--- Journal du job (20 dernières lignes)"
  docker compose exec -T spark-master tail -n 20 $LOG 2>/dev/null | grep -v -E "WARN|INFO" || true
  echo; echo "--- Base de restitution"
  $PSQL -c "SELECT COUNT(*) AS predictions, SUM(prediction) AS reachats_predits,
                   ROUND(AVG(proba_rachat)::numeric, 3) AS proba_moyenne,
                   ROUND(AVG(EXTRACT(EPOCH FROM scored_at - ingestion_ts))::numeric, 1) AS delai_moyen_s,
                   MAX(scored_at) AS derniere
            FROM predictions;"
}

case "${1:-start}" in
  stop)
    docker compose exec -T spark-master pkill -f predict_stream.py && echo "Job arrêté." || echo "Aucun job en cours."
    exit 0 ;;
  status)
    status; exit 0 ;;
esac

docker compose up -d --build postgres dashboard
if docker compose exec -T spark-master pgrep -f predict_stream.py >/dev/null; then
  echo "Le job de prédiction tourne déjà."
else
  echo "Démarrage du job de prédiction (1 cœur, modèle $VERSION)..."
  docker compose exec -d spark-master bash -c \
    "/opt/spark/bin/spark-submit --conf spark.cores.max=1 /opt/voguemind/streaming/predict_stream.py --version $VERSION > $LOG 2>&1"
  for i in $(seq 1 60); do
    sleep 3
    if docker compose exec -T spark-master grep -q "En écoute" $LOG 2>/dev/null; then break; fi
  done
fi
status
cat <<MSG

Le job écoute Kafka. Dans un autre Terminal, envoyer des transactions POSTÉRIEURES à l'historique :
    python3 ingestion/producer.py --start-date <date affichée par load_history> --limit 2000 --delay 0.05
Tableau de bord (rafraîchi toutes les 5 s) : http://localhost:8501
Suivi : ./scripts/run_week4.sh status    Arrêt : ./scripts/run_week4.sh stop
MSG
