#!/usr/bin/env python3
"""
Auto-generated test suite by QA Agent.
Run with: pytest test.py -v
"""
import pytest
import sys
import os
import json
import datetime
from unittest.mock import patch, MagicMock, PropertyMock

# Add source directory to path so imports work
sys.path.insert(0, os.path.dirname(__file__))

# Import modules under test
import telematics_producer

# ── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mocks environment variables for Kafka and producer configuration."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "test_topic")
    monkeypatch.setenv("KAFKA_USERNAME", "test_user")
    monkeypatch.setenv("KAFKA_PASSWORD", "test_pass")
    monkeypatch.setenv("PRODUCER_DELAY", "0.1")

@pytest.fixture
def mock_kafka_producer():
    """Mocks the KafkaProducer class."""
    with patch("telematics_producer.KafkaProducer") as mock:
        yield mock

# ── Unit Tests ────────────────────────────────────────────────

class TestTelematicsProducer:

    def test_get_producer_success(self, mock_env_vars, mock_kafka_producer):
        """Tests successful initialization of Kafka producer."""
        producer = telematics_producer.get_producer()
        assert producer is not None
        mock_kafka_producer.assert_called_once()
        args, kwargs = mock_kafka_producer.call_args
        assert kwargs['bootstrap_servers'] == "localhost:9092"
        assert kwargs['sasl_plain_username'] == "test_user"

    def test_get_producer_failure(self, mock_env_vars, mock_kafka_producer):
        """Tests Kafka producer initialization failure."""
        mock_kafka_producer.side_effect = Exception("Connection error")
        producer = telematics_producer.get_producer()
        assert producer is None

    @pytest.mark.parametrize("vehicle_id", ["TRUCK_001", "TRUCK_999", "TEST_VEHICLE"])
    def test_generate_telematics_data_structure(self, vehicle_id):
        """Tests that generated telematics data has correct structure and types."""
        data = telematics_producer.generate_telematics_data(vehicle_id)
        
        required_keys = ["vehicle_id", "speed", "lat", "lng", "isCargoAttached", "status", "timestamp"]
        for key in required_keys:
            assert key in data
            
        assert data["vehicle_id"] == vehicle_id
        assert isinstance(data["speed"], float)
        assert isinstance(data["lat"], float)
        assert isinstance(data["lng"], float)
        assert isinstance(data["isCargoAttached"], bool)
        assert data["status"] in ["online", "offline"]
        assert data["timestamp"].endswith("Z")

    def test_generate_telematics_data_ranges(self):
        """Tests that generated values are within expected ranges."""
        for _ in range(100):
            data = telematics_producer.generate_telematics_data("TEST")
            assert 0 <= data["speed"] <= 110
            assert -90 <= data["lat"] <= 90
            assert -180 <= data["lng"] <= 180

class TestMainLogic:

    @patch("telematics_producer.get_producer")
    @patch("telematics_producer.time.sleep")
    @patch("telematics_producer.print")
    def test_main_producer_failure(self, mock_print, mock_sleep, mock_get_producer):
        """Tests main function behavior when producer fails to initialize."""
        mock_get_producer.return_return_value = None
        telematics_producer.main()
        mock_print.assert_any_call("Failed to create producer. Exiting.")

    @patch("telematics_producer.get_producer")
    @patch("telematics_producer.time.sleep")
    @patch("telematics_producer.generate_telematics_data")
    def test_main_execution_loop(self, mock_gen, mock_sleep, mock_get_producer):
        """Tests main loop execution and message sending."""
        mock_producer = MagicMock()
        mock_get_producer.return_value = mock_producer
        
        # Mock future returned by producer.send
        mock_future = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topic = "test_topic"
        mock_metadata.partition = 0
        mock_future.get.return_value = mock_metadata
        mock_producer.send.return_value = mock_future

        # Use side_effect on sleep to raise KeyboardInterrupt and break the while loop
        mock_sleep.side_effect = KeyboardInterrupt()

        telematics_producer.main()

        # Should have attempted to send data for 5 trucks (TRUCK_001 to TRUCK_005)
        assert mock_producer.send.call_count == 5
        assert mock_producer.close.called
        assert mock_gen.call_count == 5

    @patch("telematics_producer.get_producer")
    @patch("telematics_producer.time.sleep")
    @patch("telematics_producer.print")
    def test_main_send_exception_handling(self, mock_print, mock_sleep, mock_get_producer):
        """Tests that exceptions during individual message sending are handled."""
        mock_producer = MagicMock()
        mock_get_producer.return_value = mock_producer
        
        # Mock send to raise exception
        mock_producer.send.side_effect = Exception("Send failed")
        
        mock_sleep.side_effect = KeyboardInterrupt()

        telematics_producer.main()

        # Check if error message was printed
        error_calls = [call for call in mock_print.call_args_list if "Error producing message" in str(call)]
        assert len(error_calls) > 0

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
