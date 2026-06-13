
import os

from dotenv import load_dotenv
import requests
from google.transit import gtfs_realtime_pb2
from confluent_kafka import Producer
import socket

load_dotenv()

# Read the API key from the environment variable
api_key = os.getenv("TRAFIKLAB_REALTIME_API_KEY")

TOPIC = 'gtfs-realtime-updates'

# Kafka producer configuration
conf = {'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), # Kafka broker address
        'client.id': socket.gethostname()}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Fetch GTFS real-time data
feed = gtfs_realtime_pb2.FeedMessage()
response = requests.get(f"https://opendata.samtrafiken.se/gtfs-rt-sweden/ul/TripUpdatesSweden.pb?key={api_key}")
response.raise_for_status()  # Check if the request was successful

feed.ParseFromString(response.content)
for entity in feed.entity:
    if entity.HasField('trip_update'):
        producer.produce(TOPIC, entity.SerializeToString(), callback=delivery_report)
        producer.poll(0)



producer.flush()

    

