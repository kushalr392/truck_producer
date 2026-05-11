import json
import time
import random
import os
import logging
from datetime import datetime
from confluent_kafka import Producer

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_env_vars():
    """Load configuration from env_vars.json if it exists, otherwise use environment variables."""
    config_path = 'env_vars.json'
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return json.load(f)
    return {
        "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "TOPIC_NAME": os.getenv("TOPIC_NAME"),
        "KAFKA_SASL_USERNAME": os.getenv("KAFKA_SASL_USERNAME"),
        "KAFKA_SASL_PASSWORD": os.getenv("KAFKA_SASL_PASSWORD")
    }

def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_telematics(vehicle_id):
    """Generate mock truck telematics data."""
    return {
        "vehicle_id": vehicle_id,
        "speed": round(random.uniform(0, 110), 2),
        "lat": round(random.uniform(-90, 90), 6),
        "lng": round(random.uniform(-180, 180), 6),
        "isCargoAttached": random.choice([True, False]),
        "status": random.choice(["online", "offline"]),
        "timestamp": datetime.utcnow().isoformat()
    }

def main():
    config = load_env_vars()
    
    kafka_conf = {
        'bootstrap.servers': config['KAFKA_BOOTSTRAP_SERVERS'],
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'SCRAM-SHA-512',
        'sasl.username': config['KAFKA_SASL_USERNAME'],
        'sasl.password': config['KAFKA_SASL_PASSWORD'],
        'client.id': 'truck-telematics-producer',
        'retries': 5,
        'retry.backoff.ms': 500
    }

    producer = Producer(kafka_conf)
    topic = config['TOPIC_NAME']
    vehicle_ids = [f"TRUCK-{i:03d}" for i in range(1, 6)]

    logger.info(f"Starting producer for topic: {topic}")

    try:
        while True:
            for v_id in vehicle_ids:
                data = generate_telematics(v_id)
                producer.produce(
                    topic, 
                    key=v_id, 
                    value=json.dumps(data).encode('utf-8'),
                    callback=delivery_report
                )
            
            # Flush to ensure messages are sent and callbacks are triggered
            producer.poll(0)
            time.sleep(2)  # Adjust frequency as needed
            
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
