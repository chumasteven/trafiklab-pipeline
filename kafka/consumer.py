from confluent_kafka import Consumer
import sys
from confluent_kafka import KafkaError, KafkaException
from google.cloud import storage
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file


# configure the path to your Google Cloud credentials
google_credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'writer',
        'auto.offset.reset': 'smallest'}

running = True # This variable will be used to control the consumer loop and allow for graceful shutdown.

def msg_process(msg):
    data = msg.value()
    file_name = f"gtfs_realtime_{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}.pb" # Create a unique file name based on the current timestamp
    client = storage.Client.from_service_account_json(
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
    bucket = client.bucket("trafiklab-raw-data")
    blob = bucket.blob(file_name)
    blob.upload_from_string(data,content_type='application/octet-stream')
    print(f"Message processed and stored as {file_name} in Google Cloud Storage.")

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0) # Adjust the timeout as needed
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    global running
    running = False

consumer = Consumer(conf)
topics = ['gtfs-realtime-updates']
basic_consume_loop(consumer, topics)
