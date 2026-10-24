#!/usr/bin/env bash
# =============================================================================
#  Vérification complète de l'intégration Spark + HDFS + Kafka
#  Usage (depuis la racine du dépôt) : ./scripts/verify_integration.sh
#  Windows : lancer depuis Git Bash ou WSL.
# =============================================================================
set -u
cd "$(dirname "$0")/.."

SUBMIT="/opt/spark/bin/spark-submit"
TESTS="/opt/voguemind/tests/integration"
PASS=0; FAIL=0
LOG_DIR="docs/test-logs"; mkdir -p "$LOG_DIR"

step() {  # step "titre" commande...
  local title="$1"; shift
  local log="$LOG_DIR/$(echo "$title" | tr ' /' '__' | tr -cd '[:alnum:]_-').log"
  echo; echo "▶ $title"
  if "$@" > "$log" 2>&1; then
    echo "  ✔ OK   (log : $log)"; PASS=$((PASS+1))
  else
    echo "  ✘ ECHEC (log : $log)"; tail -n 15 "$log" | sed 's/^/    /'; FAIL=$((FAIL+1))
  fi
  grep -E "^\[(OK|ECHEC)\]|RESULTAT|OK |ECHEC " "$log" | sed 's/^/    /' || true
}

echo "=== 1. État des conteneurs ==="
docker compose ps --format "table {{.Name}}\t{{.State}}\t{{.Status}}"

step "Spark Master voit 2 workers ALIVE" docker compose exec -T spark-master python3 $TESTS/check_spark_workers.py

step "Reseau depuis spark-master"   docker compose exec -T spark-master   python3 $TESTS/check_network.py
step "Reseau depuis spark-worker-1" docker compose exec -T spark-worker-1 python3 $TESTS/check_network.py
step "Reseau depuis spark-worker-2" docker compose exec -T spark-worker-2 python3 $TESTS/check_network.py

step "HDFS 2 DataNodes actifs" bash -c \
  "docker compose exec -T namenode hdfs dfsadmin -report | tee /dev/stderr | grep -q 'Live datanodes (2)'"

step "Topics Kafka" bash -c \
  "docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list | tee /dev/stderr | grep -q fashion-transactions"

step "Job Spark distribue"     docker compose exec -T spark-master $SUBMIT $TESTS/test_spark_cluster.py
step "Spark vers HDFS"         docker compose exec -T spark-master $SUBMIT $TESTS/test_hdfs_integration.py

step "Replication HDFS des fichiers Spark" bash -c \
  "docker compose exec -T namenode hdfs fsck /voguemind/tests/spark_hdfs_check -files -blocks -locations \
   | tee /dev/stderr | grep -E 'Average block replication:\s+2\.0'"

step "Kafka vers Spark Streaming vers HDFS" docker compose exec -T spark-master $SUBMIT $TESTS/test_kafka_integration.py

# Optionnel : transactions H&M réelles envoyées par ingestion/producer.py
if [ "${WITH_PRODUCER:-0}" = "1" ]; then
  step "Transactions HM reelles lues par Spark" docker compose exec -T spark-master $SUBMIT $TESTS/read_fashion_transactions.py
else
  echo; echo "ℹ Test des vraies transactions H&M ignoré (lancer producer.py puis : WITH_PRODUCER=1 ./scripts/verify_integration.sh)"
fi

echo; echo "======================================"
echo " Bilan : $PASS réussi(s), $FAIL échec(s)"
echo "======================================"
[ "$FAIL" -eq 0 ]
