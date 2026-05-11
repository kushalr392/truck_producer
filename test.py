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
from producer import load_env_vars, on_send_success, on_send_error, generate_telematics, main

# ── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def mock_env_vars():
    """Provides a standard set of mock environment variables."""
    return {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "TOPIC_NAME": "test-topic",
        "KAFKA_SASL_USERNAME": "user",
        "KAFKA_SASL_PASSWORD": "password"
    }

@pytest.fixture
def mock_record_metadata():
    """Provides a mock Kafka record metadata object."""
    metadata = MagicMock()
    metadata.topic = "test-topic"
    metadata.partition = 0
    metadata.offset = 123
    return metadata

# ── Unit Tests ────────────────────────────────────────────────

class TestEnvLoading:
    """Tests for environment variable and config loading."""

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data='{"KAFKA_BOOTSTRAP_SERVERS": "file_server", "TOPIC_NAME": "file_topic"}')
    def test_load_env_vars_from_file(self, mock_file, mock_exists):
        """Tests that config is loaded from env_vars.json if it exists."""
        mock_exists.return_value = True
        config = load_env_vars()
        assert config["KAFKA_BOOTSTRAP_SERVERS"] == "file_server"
        assert config["TOPIC_NAME"] == "file_topic"
        mock_file.assert_called_with('env_vars.json', 'r')

    @patch('os.path.exists')
    @patch('os.getenv')
    def test_load_env_vars_from_env(self, mock_getenv, mock_exists):
        """Tests that config is loaded from environment variables if file is missing."""
        mock_exists.return_value = False
        
        def side_effect(key, default=None):
            env = {
                "KAFKA_BOOTSTRAP_SERVERS": "env_server",
                "TOPIC_NAME": "env_topic",
                "KAFKA_SASL_USERNAME": "env_user",
                "KAFKA_SASL_PASSWORD": "env_pass"
            }
            return env.get(key)
        
        mock_getenv.side_effect = side_effect
        
        config = load_env_vars()
        assert config["KAFKA_BOOTSTRAP_SERVERS"] == "env_server"
        assert config["TOPIC_NAME"] == "env_topic"
        assert config["KAFKA_SASL_USERNAME"] == "env_user"

class TestCallbacks:
    """Tests for Kafka producer callbacks."""

    @patch('producer.logger')
    def test_on_send_success(self, mock_logger, mock_record_metadata):
        """Tests successful message delivery logging."""
        on_send_success(mock_record_metadata)
        mock_logger.info.assert_called()
        log_msg = mock_logger.info.call_args[0][0]
        assert "test-topic" in log_msg
        assert "offset 123" in log_msg

    @patch('producer.logger')
    def test_on_send_error(self, mock_logger):
        """Tests failed message delivery logging."""
        exception = Exception("Kafka Error")
        on_send_error(exception)
        mock_logger.error.assert_called_with("Message delivery failed: Kafka Error")

class TestTelematicsGeneration:
    """Tests for telematics data generation."""

    def test_generate_telematics_structure(self):
        """Tests the structure and required keys of generated telematics."""
        v_id = "TRUCK-001"
        data = generate_telematics(v_id)
        
        expected_keys = {"vehicle_id", "speed", "lat", "lng", "isCargoAttached", "status", "timestamp"}
        assert set(data.keys()) == expected_keys
        assert data["vehicle_id"] == v_id

    @pytest.mark.parametrize("iteration", range(5))
    def test_generate_telematics_ranges(self, iteration):
        """Tests that generated values are within expected boundaries."""
        data = generate_telematics("TEST")
        assert 0 <= data["speed"] <= 110
        assert -90 <= data["lat"] <= 90
        assert -180 <= data["lng"] <= 180
        assert data["status"] in ["online", "offline"]
        assert isinstance(data["isCargoAttached"], bool)

# ── Integration & Workflow Tests ──────────────────────────────

class TestMainWorkflow:
    """Tests for the main execution loop and setup."""

    @patch('producer.KafkaProducer')
    @patch('producer.load_env_vars')
    @patch('producer.time.sleep', side_effect=KeyboardInterrupt)  # Break the infinite loop immediately
    def test_main_startup_and_keyboard_interrupt(self, mock_sleep, mock_load, mock_producer_class, mock_env_vars):
        """Tests that main initializes producer and handles KeyboardInterrupt gracefully."""
        mock_load.return_value = mock_env_vars
        mock_producer_instance = mock_producer_class.return_value
        
        # Run main - it will trigger KeyboardInterrupt on first sleep
        main()
        
        # Check initialization
        mock_producer_class.assert_called_once()
        args, kwargs = mock_producer_class.call_args
        assert kwargs['bootstrap_servers'] == ['localhost:9092']
        assert kwargs['client_id'] == 'truck-telematics-producer'
        
        # Verify cleanup
        mock_producer_instance.close.assert_called_once()

    @patch('producer.KafkaProducer')
    @patch('producer.load_env_vars')
    @patch('producer.time.sleep')
    @patch('producer.generate_telematics')
    def test_main_message_sending(self, mock_gen, mock_sleep, mock_load, mock_producer_class, mock_env_vars):
        """Tests that messages are sent for all vehicles in the list."""
        mock_load.return_value = mock_env_vars
        mock_producer_instance = mock_producer_class.return_value
        mock_gen.return_value = {"data": "mock"}
        
        # We need to stop the loop after one iteration
        # We can raise an exception after 5 calls (for 5 trucks) to mock_gen
        mock_gen.side_effect = [{"data": f"truck_{i}"} for i in range(5)] + [KeyboardInterrupt()]
        
        try:
            main()
        except KeyboardInterrupt:
            pass
            
        # 5 vehicles (TRUCK-001 to TRUCK-005)
        assert mock_producer_instance.send.call_count == 5
        mock_producer_instance.send.assert_any_call(
            mock_env_vars["TOPIC_NAME"], 
            key="TRUCK-001", 
            value={"data": "truck_0"}
        )

    @patch('producer.KafkaProducer', side_effect=Exception("Connection failed"))
    @patch('producer.load_env_vars')
    @patch('producer.logger')
    def test_main_init_failure(self, mock_logger, mock_load, mock_producer_class, mock_env_vars):
        """Tests that initialization errors are caught and logged."""
        mock_load.return_value = mock_env_vars
        
        main()
        
        mock_logger.error.assert_called()
        assert "Failed to initialize Kafka producer" in mock_logger.error.call_args[0][0]

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
