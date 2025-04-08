import pandas as pd

# Charger le fichier CSV
 
df = pd.read_csv('C:/Users/Amal/projects/Car Prise DataSet.csv',sep=';')
print(df.head())



# Afficher les premières lignes du dataset
print(df.head())

# Vérifier les types de données
print(df.info())

from confluent_kafka import Producer
import pandas as pd
import json

# Chargement du dataset
df = pd.read_csv('car_price_data.csv')

# Configuration du producteur Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker
    'client.id': 'car-price-producer'
}

# Création d'un producteur Kafka
producer = Producer(conf)

# Fonction de callback pour vérifier la livraison du message
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Envoyer chaque ligne du dataset dans Kafka
for index, row in df.iterrows():
    # Convertir chaque ligne en dictionnaire (JSON)
    car_data = row.to_dict()
    # Convertir le dictionnaire en string JSON
    car_data_json = json.dumps(car_data)

    # Envoyer le message dans Kafka
    producer.produce('car-price-topic', key=str(index), value=car_data_json, callback=delivery_report)

# Attendre que tous les messages soient envoyés
producer.flush()
