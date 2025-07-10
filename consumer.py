from confluent_kafka import Consumer, KafkaException

# Configuration du consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'  # lire depuis le début de topic
}

# Création du consumer
consumer = Consumer(conf)

# Abonnement au topic
consumer.subscribe(['benidevoir4'])

print("🟢 En attente des messages... (Ctrl+C pour arrêter)\n")

try:
    while True:
        msg = consumer.poll(1.0)  # Attend jusqu'à 1 seconde
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            print(f"✅ Message reçu: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("⛔ Arrêt demandé par l'utilisateur.")
finally:
    consumer.close()
    print("🧹 Connexion fermée.")