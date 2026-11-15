
import pandas as pd
import json
import time
import os
import argparse
from pathlib import Path
from datetime import datetime, timezone
from kafka import KafkaProducer
import sys
from kafka.errors import KafkaError

# Configuration
ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / "data" / "transactions_train.csv"

TOPIC = "fashion-transactions"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

parser = argparse.ArgumentParser()
parser.add_argument("--limit", type=int, default=1000)
parser.add_argument("--delay", type=float, default=0.1)
parser.add_argument("--start-date", default=None,
                    help="rejoue les transactions à partir de cette date (AAAA-MM-JJ)")
args = parser.parse_args()

if args.limit <= 0 or args.delay < 0:
    parser.error("--limit doit être positif et --delay >= 0")

# Connexion Kafka avec gestion des erreurs
try:
    print(f"[INFO] Connexion à Kafka : {BOOTSTRAP}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda value: json.dumps(
            value
        ).encode("utf-8"),
        acks="all",
        retries=3,
        request_timeout_ms=10000,
        bootstrap_timeout_ms=10000
    )

    print("[OK] Connexion Kafka établie")

except KafkaError as error:
    print(f"[ERREUR] Impossible de se connecter à Kafka : {error}")
    sys.exit(1)

# Lecture d'un échantillon H&M
if args.start_date is None:
    transactions = pd.read_csv(
        CSV_PATH,
        nrows=args.limit,
        dtype={
            "customer_id": str,
            "article_id": str
        }
    )
else:
    # Le fichier est trié par date : lecture par blocs jusqu'à la date demandée.
    # L'index d'origine est conservé pour garder des transaction_id uniques.
    selected = []
    remaining = args.limit
    for chunk in pd.read_csv(
        CSV_PATH,
        chunksize=200_000,
        dtype={"customer_id": str, "article_id": str}
    ):
        chunk = chunk[chunk["t_dat"] >= args.start_date]
        if len(chunk):
            selected.append(chunk.head(remaining))
            remaining -= len(selected[-1])
            if remaining <= 0:
                break
    transactions = pd.concat(selected) if selected else pd.DataFrame()
    print(f"[INFO] Rejeu à partir du {args.start_date}")

sent = 0
failed = 0

print(f"Topic : {TOPIC}")
print(f"Transactions à traiter : {len(transactions)}")

try:
    for index, row in transactions.iterrows():
        try:
            event = {
                "transaction_id": f"tx_{index}",
                "customer_id": row["customer_id"],
                "article_id": row["article_id"],
                "timestamp": row["t_dat"],
                "price": float(row["price"]),
                "sales_channel_id": int(row["sales_channel_id"]),
                "event_type": "purchase",
                "ingestion_timestamp": datetime.now(
                    timezone.utc
                ).isoformat()
            }

            result = producer.send(
                TOPIC,
                value=event
            ).get(timeout=15)

            sent += 1

            print(
                f"[OK] {sent} | "
                f"partition={result.partition} | "
                f"offset={result.offset}"
            )

        except (ValueError, TypeError, KeyError) as error:
            failed += 1
            print(f"[INVALID DATA] Ligne {index}: {error}")

        except Exception as error:
            failed += 1
            print(f"[KAFKA ERROR] Ligne {index}: {error}")
            raise

        time.sleep(args.delay)

finally:
    producer.flush()
    producer.close()

    print("\n===== BILAN INGESTION =====")
    print(f"Transactions envoyées : {sent}")
    print(f"Transactions échouées : {failed}")
    print("===========================")
