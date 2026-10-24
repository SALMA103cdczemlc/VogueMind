"""
Test de communication réseau entre les conteneurs (sans Spark).

Vérifie, depuis le conteneur où il est lancé, la résolution DNS Docker
et l'ouverture des ports de chaque service du cluster VogueMind.

Usage :
    docker compose exec spark-master python3 /opt/voguemind/tests/integration/check_network.py
"""
import socket
import sys

SERVICES = [
    # (service, hôte, port, rôle)
    ("Spark Master (cluster)", "spark-master", 7077, "soumission des jobs"),
    ("Spark Master (UI)", "spark-master", 8080, "interface web"),
    ("Spark Worker 1 (UI)", "spark-worker-1", 8081, "worker 1"),
    ("Spark Worker 2 (UI)", "spark-worker-2", 8081, "worker 2"),
    ("HDFS NameNode (RPC)", "namenode", 9000, "fs.defaultFS"),
    ("HDFS NameNode (UI)", "namenode", 9870, "interface web"),
    ("HDFS DataNode 1", "datanode1", 9864, "stockage bloc"),
    ("HDFS DataNode 2", "datanode2", 9864, "stockage bloc"),
    ("Kafka broker", "kafka", 29092, "listener interne"),
]


def check(host: str, port: int, timeout: float = 3.0):
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror as exc:
        return False, f"DNS KO ({exc})"
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True, ip
    except OSError as exc:
        return False, f"{ip} port fermé ({exc})"


def main() -> int:
    me = socket.gethostname()
    print(f"\n=== Test réseau depuis le conteneur '{me}' ===\n")
    print(f"{'Service':<26}{'Cible':<24}{'Résultat':<8} Détail")
    print("-" * 80)
    failures = 0
    for name, host, port, _role in SERVICES:
        ok, detail = check(host, port)
        failures += 0 if ok else 1
        print(f"{name:<26}{host + ':' + str(port):<24}{'OK' if ok else 'ECHEC':<8} {detail}")
    print("-" * 80)
    if failures:
        print(f"RESULTAT : {failures} service(s) injoignable(s)")
        return 1
    print("RESULTAT : tous les services sont joignables")
    return 0


if __name__ == "__main__":
    sys.exit(main())
