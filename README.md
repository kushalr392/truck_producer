# Truck Telematics Simulator

This Python script simulates truck telematics data and produces it to a Kafka topic.

## Data Schema
- `vehicle_id`: Unique identifier for the truck.
- `speed`: Current speed in km/h.
- `lat`: Latitude.
- `lng`: Longitude.
- `isCargoAttached`: Boolean indicating if cargo is attached.
- `status`: "online" or "offline".
- `timestamp`: UTC timestamp.

## Setup

### Environment Variables
Create a `.env` file or export the following variables:
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses.
- `KAFKA_TOPIC`: Kafka topic name.
- `KAFKA_USERNAME`: SASL username.
- `KAFKA_PASSWORD`: SASL password.
- `PRODUCER_DELAY`: (Optional) Delay between messages in seconds (default: 1.0).

### Running Locally
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Set environment variables.
3. Run the script:
   ```bash
   python telematics_producer.py
   ```

### Running with Docker
1. Build the image:
   ```bash
   docker build -t truck-telematics-simulator .
   ```
2. Run the container:
   ```bash
   docker run --env-file .env truck-telematics-simulator
   ```
