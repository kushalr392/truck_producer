#!/usr/bin/env python3
"""
Auto-generated test suite by QA Agent.
Run with: pytest test.py -v
"""
import pytest
import sys
import os
import json
from unittest.mock import patch, MagicMock, mock_open

# Add source directory to path so imports work
sys.path.insert(0, os.path.dirname(__file__))

# Import modules under test
import producer

# ── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def mock_kafka_conf():
    return {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "TOPIC_NAME": "test-topic",
        "KAFKA_SASL_USERNAME": "user",
        "KAFKA_SASL_PASSWORD": "password"
    }

@pytest.fixture
def mock_telematics_data():
    return {
        "vehicle_id": "TRUCK-001",
        "speed": 55.5,
        "lat": 12.34,
        "lng": 56.78,
        "isCargoAttached": True,
        "status": "online",
        "timestamp": "2023-01-01T00:00:00"
    }

# ── Unit Tests ────────────────────────────────────────────────

class TestEnvVars:
    @patch("os.path.exists")
    @patch("builtins.open", new_callable=mock_open, read_data='{"KAFKA_BOOTSTRAP_SERVERS": "file_server"}')
    def test_load_env_vars_from_file(self, mock_file, mock_exists):
        """Tests that config is loaded from env_vars.json if it exists."""
        mock_exists.return_value = True
        config = producer.load_env_vars()
        assert config["KAFKA_BOOTSTRAP_SERVERS"] == "file_server"
        mock_file.assert_called_with("env_vars.json", "r")

    @patch("os.path.exists")
    @patch("os.getenv")
    def test_load_env_vars_from_env(self, mock_getenv, mock_exists):
        """Tests that config is loaded from environment variables if file is missing."""
        mock_exists.return_value = False
        mock_getenv.side_effect = lambda k: f"env_{k}"
        
        config = producer.load_env_vars()
        assert config["KAFKA_BOOTSTRAP_SERVERS"] == "env_KAFKA_BOOTSTRAP_SERVERS"
        assert config["TOPIC_NAME"] == "env_TOPIC_NAME"

class TestProducerCallbacks:
    @patch("producer.logger")
    def test_delivery_report_success(self, mock_logger):
        """Tests successful delivery report logging."""
        msg = MagicMock()
        msg.topic.return_value = "test-topic"
        msg.partition.return_value = 0
        
        producer.delivery_report(None, msg)
        mock_logger.info.assert_called_with("Message delivered to test-topic [0]")

    @patch("producer.logger")
    def test_delivery_report_error(self, mock_logger):
        """Tests delivery report logging when an error occurs."""
        msg = MagicMock()
        err = "Connection error"
        
        producer.delivery_report(err, msg)
        mock_logger.error.assert_called_with(f"Message delivery failed: {err}")

class TestDataGeneration:
    def test_generate_telematics_structure(self):
        """Tests the structure and types of generated telematics data."""
        v_id = "TRUCK-999"
        data = producer.generate_telematics(v_id)
        
        assert data["vehicle_id"] == v_id
        assert isinstance(data["speed"], (int, float))
        assert isinstance(data["lat"], float)
        assert isinstance(data["lng"], float)
        assert isinstance(data["isCargoAttached"], bool)
        assert data["status"] in ["online", "offline"]
        assert "timestamp" in data

    @patch("random.uniform")
    @patch("random.choice")
    def test_generate_telematics_values(self, mock_choice, mock_uniform):
        """Tests telematics generation with specific mocked values."""
        mock_uniform.side_effect = [60.0, 10.0, 20.0]
        mock_choice.side_effect = [True, "online"]
        
        data = producer.generate_telematics("TRUCK-001")
        
        assert data["speed"] == 60.0
        assert data["lat"] == 10.0
        assert data["lng"] == 20.0
        assert data["isCargoAttached"] is True
        assert data["status"] == "online"

# ── Integration/Main Tests ────────────────────────────────────

class TestMain:
    @patch("producer.Producer")
    @patch("producer.load_env_vars")
    @patch("producer.time.sleep", side_effect=KeyboardInterrupt) # To break the loop
    def test_main_loop_execution(self, mock_sleep, mock_load, mock_producer_class):
        """Tests that main function initializes producer and attempts to send messages."""
        mock_load.return_value = {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "TOPIC_NAME": "test-topic",
            "KAFKA_SASL_USERNAME": "user",
            "KAFKA_SASL_PASSWORD": "pass"
        }
        mock_producer_instance = MagicMock()
        mock_producer_class.return_value = mock_producer_instance
        
        # Run main - it will raise KeyboardInterrupt on first sleep
        producer.main()
        
        # Verify producer was created with right config
        mock_producer_class.assert_called_once()
        args, kwargs = mock_producer_class.call_args
        assert args[0]['bootstrap.servers'] == "localhost:9092"
        
        # Verify produce was called for the 5 trucks
        assert mock_producer_instance.produce.call_count == 5
        mock_producer_instance.flush.assert_called_once()

    @patch("producer.Producer")
    @patch("producer.load_env_vars")
    @patch("producer.logger")
    def test_main_keyboard_interrupt(self, mock_logger, mock_load, mock_producer_class):
        """Tests that KeyboardInterrupt is handled gracefully."""
        mock_load.return_value = {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "TOPIC_NAME": "test-topic",
            "KAFKA_SASL_USERNAME": "user",
            "KAFKA_SASL_PASSWORD": "pass"
        }
        
        # Side effect to raise interrupt immediately inside the loop
        with patch("producer.generate_telematics", side_effect=KeyboardInterrupt):
            producer.main()
            
        mock_logger.info.assert_any_call("Stopping producer...")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
