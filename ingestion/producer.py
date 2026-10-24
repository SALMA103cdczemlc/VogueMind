
import pandas as pd
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from kafka import KafkaProducer

# Connexion à Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

# Chemin du dataset H&M
csv_path = (
    Path(__file__).resolve().parent.parent
    / "data" / "transactions_train.csv"
)

# Lire seulement 10 transactions pour commencer
transactions = pd.read_csv(
    csv_path,
    nrows=1000,
    dtype={"customer_id": str, "article_id": str}
)

# Envoyer chaque transaction dans Kafka
for index, row in transactions.iterrows():

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

    producer.send(
        "fashion-transactions",
        value=event
    ).get(timeout=10)

    print("Transaction envoyée :", event)

    time.sleep(0.01)

producer.flush()
producer.close()

print("Toutes les transactions ont été envoyées !")
