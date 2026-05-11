# Truck Telematics Kafka Producer

This project simulates a Kafka producer for truck telematics data. It generates random data for a set of vehicles and pushes it to a Kafka topic.

## Data Schema
- `vehicle_id`: Unique identifier for the truck.
- `speed`: Current speed in km/h.
- `lat`: Latitude.
- `lng`: Longitude.
- `isCargoAttached`: Boolean indicating if cargo is attached.
- `status`: "online" or "offline".
- `timestamp`: UTC ISO timestamp.

## Setup

### 1. Environment Variables
Update the `env_vars.json` file with your Kafka credentials:
- `KAFKA_BOOTSTRAP_SERVERS`
- `TOPIC_NAME`
- `KAFKA_SASL_USERNAME`
- `KAFKA_SASL_PASSWORD`

### 2. Local Execution
```bash
pip install -r requirements.txt
python producer.py
```

### 3. Docker Execution
```bash
docker build -t truck-producer .
docker run --env-file <(jq -r 'to_entries|map("\(.key)=\(.value)")|.[]' env_vars.json) truck-producer
```
*(Or simply ensure the `env_vars.json` is copied into the image, which it is by default in the Dockerfile instructions).*

## Kafka Security
This script is configured to use `SASL_PLAINTEXT` with `SCRAM-SHA-512` as per requirements.
