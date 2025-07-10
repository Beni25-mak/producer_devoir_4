from confluent_kafka import Producer
import time

# configuration du producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# fonction de callback pour vérifier la livraison des messages

def delivery_report(err, msg):
    if err is not None:
        print(f"Message non livré: {err}")
    else:
        print(f"Message bien livré:{msg.value().decode()}")

# envoi message depuis une liste sur les transactions financières

messages = [
'{"transaction_id": "TX1001", "user": "beni", "montant": 25000, "devise": "CDF"}',
'{"transaction_id": "TX1002", "user": "nzimba", "montant": 800, "devise": "USD"}',
'{"transaction_id": "TX1003", "user": "paul", "montant": 250000, "devise": "FCFA"}',
'{"transaction_id": "TX1004", "user": "arnold", "montant": 530, "devise": "USD"}',
'{"transaction_id": "TX1005", "user": "alice", "montant": 1220000, "devise": "FCFA"}'
    ]

for message in messages:
    producer.produce("benidevoir4", message.encode(), callback=delivery_report)
    producer.flush()
    time.sleep(2)

print("Tous les messages ont été envoyés.")