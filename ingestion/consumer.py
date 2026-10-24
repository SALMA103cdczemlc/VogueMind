
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "fashion-transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="voguemind-test-consumer-v2"
)

print("En attente des transactions H&M...")

try:
    for message in consumer:
        try:
            transaction = json.loads(
                message.value.decode("utf-8")
            )

            print("\nNouvelle transaction reçue")
            print("Client :", transaction["customer_id"])
            print("Article :", transaction["article_id"])
            print("Prix :", transaction["price"])
            print("Date :", transaction["timestamp"])

        except (json.JSONDecodeError, UnicodeDecodeError):
            print("Message non JSON ignoré :", message.value)

        except KeyError as e:
            print("Message avec champ manquant :", e)

except KeyboardInterrupt:
    print("\nConsommateur arrêté.")

finally:
    consumer.close()
