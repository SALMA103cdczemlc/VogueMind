"""
Vérifie via l'API JSON du Spark Master que les 2 workers sont enregistrés (ALIVE).

Usage :
    docker compose exec spark-master python3 /opt/voguemind/tests/integration/check_spark_workers.py
"""
import json
import sys
import urllib.request

URL = "http://spark-master:8080/json/"
EXPECTED = 2

with urllib.request.urlopen(URL, timeout=5) as resp:
    state = json.load(resp)

alive = [w for w in state.get("workers", []) if w.get("state") == "ALIVE"]
print(f"Master : {state.get('url')}  statut : {state.get('status')}")
print(f"Workers ALIVE : {len(alive)} / attendu : {EXPECTED}")
for w in alive:
    print(f"  - {w['host']:<16} {w['cores']} cores  {w['memory']} Mo  ({w['id']})")
print(f"Cores totaux : {state.get('cores')}  Mémoire totale : {state.get('memory')} Mo")
ok = len(alive) >= EXPECTED and state.get("status") == "ALIVE"
print("OK cluster Spark complet" if ok else "ECHEC workers manquants")
sys.exit(0 if ok else 1)
