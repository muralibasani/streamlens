"""Unit tests for server/src/kafka/connectors.py."""
from unittest.mock import MagicMock, patch

import pytest

from src.kafka.connectors import (
    SENSITIVE_KEYWORDS,
    fetch_connector_details,
    fetch_connectors,
)


class TestFetchConnectors:
    """Tests for fetch_connectors(connect_url)."""

    @patch("src.kafka.connectors.httpx.Client")
    def test_returns_connector_list_with_topics(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp_names = MagicMock()
        mock_resp_names.json.return_value = ["jdbc-sink-orders"]
        mock_resp_names.raise_for_status = MagicMock()

        mock_resp_connector = MagicMock()
        mock_resp_connector.json.return_value = {
            "name": "jdbc-sink-orders",
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                "topics": "orders,events",
                "connection.url": "jdbc:postgresql://db:5432/mydb",
            },
            "tasks": [{"connector": "jdbc-sink-orders", "task": 0}],
        }
        mock_resp_connector.raise_for_status = MagicMock()

        mock_client.get.side_effect = [mock_resp_names, mock_resp_connector]

        result = fetch_connectors("http://localhost:8083")
        assert len(result) >= 1
        assert any(c["id"] == "connect:jdbc-sink-orders" for c in result)

    @patch("src.kafka.connectors.httpx.Client")
    def test_sink_connector_type(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp_names = MagicMock()
        mock_resp_names.json.return_value = ["my-sink"]
        mock_resp_names.raise_for_status = MagicMock()

        mock_resp_connector = MagicMock()
        mock_resp_connector.json.return_value = {
            "name": "my-sink",
            "type": "sink",
            "config": {"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", "topics": "orders"},
            "tasks": [],
        }
        mock_resp_connector.raise_for_status = MagicMock()

        mock_client.get.side_effect = [mock_resp_names, mock_resp_connector]

        result = fetch_connectors("http://localhost:8083")
        sink_connectors = [c for c in result if c["type"] == "sink"]
        assert len(sink_connectors) >= 1

    @patch("src.kafka.connectors.httpx.Client")
    def test_source_connector_type(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp_names = MagicMock()
        mock_resp_names.json.return_value = ["my-source"]
        mock_resp_names.raise_for_status = MagicMock()

        mock_resp_connector = MagicMock()
        mock_resp_connector.json.return_value = {
            "name": "my-source",
            "type": "source",
            "config": {"connector.class": "io.confluent.connect.file.FileStreamSourceConnector", "topic": "events"},
            "tasks": [],
        }
        mock_resp_connector.raise_for_status = MagicMock()

        mock_client.get.side_effect = [mock_resp_names, mock_resp_connector]

        result = fetch_connectors("http://localhost:8083")
        source_connectors = [c for c in result if c["type"] == "source"]
        assert len(source_connectors) >= 1

    @patch("src.kafka.connectors.httpx.Client")
    def test_deduplication_of_same_id_topic(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp_names = MagicMock()
        mock_resp_names.json.return_value = ["dedup-connector"]
        mock_resp_names.raise_for_status = MagicMock()

        mock_resp_connector = MagicMock()
        mock_resp_connector.json.return_value = {
            "name": "dedup-connector",
            "config": {"connector.class": "SinkConnector", "topics": "topic1,topic1"},
            "tasks": [],
        }
        mock_resp_connector.raise_for_status = MagicMock()

        mock_client.get.side_effect = [mock_resp_names, mock_resp_connector]

        result = fetch_connectors("http://localhost:8083")
        topic1_entries = [c for c in result if c["topic"] == "topic1"]
        assert len(topic1_entries) == 1


class TestFetchConnectorDetails:
    """Tests for fetch_connector_details(connect_url, connector_name)."""

    @patch("src.kafka.connectors.httpx.Client")
    def test_returns_config_with_sensitive_values_masked(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "name": "jdbc-sink",
            "type": "sink",
            "config": {
                "connector.class": "JdbcSinkConnector",
                "connection.password": "secret123",
                "topics": "orders",
            },
            "tasks": [],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_resp

        result = fetch_connector_details("http://localhost:8083", "jdbc-sink")
        assert result["config"]["connection.password"] == "********"
        assert result["config"]["topics"] == "orders"

    @patch("src.kafka.connectors.httpx.Client")
    def test_password_fields_masked(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "name": "test",
            "config": {"password": "mypass", "api.key": "key123"},
            "tasks": [],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_resp

        result = fetch_connector_details("http://localhost:8083", "test")
        assert result["config"]["password"] == "********"
        assert result["config"]["api.key"] == "********"

    @patch("src.kafka.connectors.httpx.Client")
    def test_non_sensitive_fields_kept(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "name": "test",
            "config": {"connector.class": "JdbcSink", "topics": "orders", "batch.size": "1000"},
            "tasks": [],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_resp

        result = fetch_connector_details("http://localhost:8083", "test")
        assert result["config"]["connector.class"] == "JdbcSink"
        assert result["config"]["topics"] == "orders"
        assert result["config"]["batch.size"] == "1000"


class TestSensitiveKeywords:
    """Tests for SENSITIVE_KEYWORDS constant."""

    def test_sensitive_keywords_defined(self):
        assert "password" in SENSITIVE_KEYWORDS
        assert "secret" in SENSITIVE_KEYWORDS
        assert "token" in SENSITIVE_KEYWORDS
