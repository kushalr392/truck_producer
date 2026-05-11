import json
import time
import random
import os
import logging
from datetime import datetime
from kafka import KafkaProducer

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

def on_send_success(record_metadata):
    """Success callback for message delivery."""
    logger.info(f"Message delivered to {record_metadata.topic} [{record_metadata.partition}] offset {record_metadata.offset}")

def on_send_error(excp):
    """Error callback for message delivery."""
    logger.error(f"Message delivery failed: {excp}")

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
    
    # Initialize KafkaProducer
    try:
        producer = KafkaProducer(
            bootstrap_servers=config['KAFKA_BOOTSTRAP_SERVERS'].split(','),
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='SCRAM-SHA-512',
            sasl_plain_username=config['KAFKA_SASL_USERNAME'],
            sasl_plain_password=config['KAFKA_SASL_PASSWORD'],
            retries=5,
            retry_backoff_ms=500,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            client_id='truck-telematics-producer'
        )
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        return

    topic = config['TOPIC_NAME']
    vehicle_ids = [f"TRUCK-{i:03d}" for i in range(1, 6)]

    logger.info(f"Starting producer for topic: {topic}")

    try:
        while True:
            for v_id in vehicle_ids:
                data = generate_telematics(v_id)
                # Async send
                future = producer.send(topic, key=v_id, value=data)
                
                # Add callbacks
                future.add_callback(on_send_success)
                future.add_errback(on_send_error)
            
            time.sleep(2)  # Adjust frequency as needed
            
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        # Close producer to flush messages
        producer.close(timeout=10)

if __name__ == "__main__":
    main()
