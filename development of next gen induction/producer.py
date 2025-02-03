import time
import json
import random
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_TOPIC = "machine_sensor_data"
KAFKA_SERVER = "localhost:9092"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sensor_data():
    """Simulate IoT sensor data for industrial machines."""
    return {
        "machine_id": random.randint(1, 10),
        "temperature": round(random.uniform(100, 500), 2),
        "voltage": round(random.uniform(200, 240), 2),
        "induction_cycle": random.randint(1, 100),
        "timestamp": time.time()
    }

while True:
    data = generate_sensor_data()
    producer.send(KAFKA_TOPIC, data)
    logger.info(f"Sent: {data}")
    time.sleep(2)  # Simulate real-time streaming