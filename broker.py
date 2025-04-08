from confluent_kafka import Consumer, KafkaException, KafkaError
import json

# Configuration du consommateur Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',   # Kafka broker
    'group.id': 'car-price-consumer-group',   # Identifiant du groupe de consommateurs
    'auto.offset.reset': 'earliest'           # Lire depuis le début du topic
}

# Création du consommateur Kafka
consumer = Consumer(conf)

# Souscrire au topic
consumer.subscribe(['car-price-topic'])

# Consommer les messages
try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Attendre un message pendant 1 seconde
        if msg is None:
            continue  # Aucun message
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Fin de la partition, continuer
                continue
            else:
                raise KafkaException(msg.error())
        
        # Afficher le message
        car_data = json.loads(msg.value().decode('utf-8'))
        print(f"Consommé: {car_data}")

except KeyboardInterrupt:
    pass
finally:
    # Fermer le consommateur
    consumer.close()
