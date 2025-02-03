from kafka import KafkaConsumer
import json
import pymongo
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_TOPIC = "machine_sensor_data"
KAFKA_SERVER = "localhost:9092"

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# MongoDB Connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["iiot_database"]
collection = db["sensor_data"]

for message in consumer:
    data = message.value
    logger.info(f"Received: {data}")
    collection.insert_one(data)  # Store in MongoDB
    logger.info(f"Inserted into MongoDB: {data}")