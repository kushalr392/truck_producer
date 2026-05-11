import os
import json
import time
import random
import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('KAFKA_TOPIC', 'truck_telematics')
USERNAME = os.getenv('KAFKA_USERNAME')
PASSWORD = os.getenv('KAFKA_PASSWORD')
DELAY = float(os.getenv('PRODUCER_DELAY', 1.0))

def get_producer():
    """Initializes and returns a Kafka producer with SASL authentication."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='SCRAM-SHA-512',
            sasl_plain_username=USERNAME,
            sasl_plain_password=PASSWORD,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        return producer
    except Exception as e:
        print(f"Error initializing Kafka producer: {e}")
        return None

def generate_telematics_data(vehicle_id):
    """Generates random truck telematics data."""
    return {
        "vehicle_id": vehicle_id,
        "speed": round(random.uniform(0, 110), 2),
        "lat": round(random.uniform(-90, 90), 6),
        "lng": round(random.uniform(-180, 180), 6),
        "isCargoAttached": random.choice([True, False]),
        "status": random.choice(["online", "offline"]),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
    }

def main():
    print("Starting Truck Telematics Simulator...")
    producer = get_producer()
    
    if not producer:
        print("Failed to create producer. Exiting.")
        return

    vehicle_ids = [f"TRUCK_{str(i).zfill(3)}" for i in range(1, 6)]
    
    try:
        while True:
            for v_id in vehicle_ids:
                data = generate_telematics_data(v_id)
                try:
                    future = producer.send(TOPIC, value=data)
                    # Wait for record metadata or error
                    record_metadata = future.get(timeout=10)
                    print(f"Produced: {data['vehicle_id']} to {record_metadata.topic} partition {record_metadata.partition}")
                except Exception as e:
                    print(f"Error producing message for {v_id}: {e}")
            
            time.sleep(DELAY)
    except KeyboardInterrupt:
        print("Stopping simulator...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
