"""Unit tests for server/src/kafka/schemas.py."""
from unittest.mock import MagicMock, patch

import pytest

from src.kafka.schemas import fetch_schema_details, fetch_schemas


class TestFetchSchemas:
    """Tests for fetch_schemas(schema_url)."""

    @patch("src.kafka.schemas.httpx.Client")
    def test_returns_subjects_and_version_details(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp_subjects = MagicMock()
        mock_resp_subjects.json.return_value = ["orders-value"]
        mock_resp_subjects.raise_for_status = MagicMock()

        mock_resp_version = MagicMock()
        mock_resp_version.json.return_value = {
            "version": 1,
            "id": 10,
            "schemaType": "AVRO",
        }
        mock_resp_version.raise_for_status = MagicMock()

        mock_client.get.side_effect = [mock_resp_subjects, mock_resp_version]

        result = fetch_schemas("http://localhost:8081")
        assert len(result) == 1
        assert result[0]["subject"] == "orders-value"
        assert result[0]["version"] == 1
        assert result[0]["id"] == 10
        assert result[0]["type"] == "AVRO"
        assert result[0]["topicName"] == "orders"

    @patch("src.kafka.schemas.httpx.Client")
    def test_empty_subjects_returns_empty_list(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_resp

        result = fetch_schemas("http://localhost:8081")
        assert result == []

    @patch("src.kafka.schemas.httpx.Client")
    def test_subject_key_suffix_stripped_from_topic_name(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp_subjects = MagicMock()
        mock_resp_subjects.json.return_value = ["events-key"]
        mock_resp_subjects.raise_for_status = MagicMock()

        mock_resp_version = MagicMock()
        mock_resp_version.json.return_value = {"version": 1, "id": 5, "schemaType": "AVRO"}
        mock_resp_version.raise_for_status = MagicMock()

        mock_client.get.side_effect = [mock_resp_subjects, mock_resp_version]

        result = fetch_schemas("http://localhost:8081")
        assert result[0]["topicName"] == "events"


class TestFetchSchemaDetails:
    """Tests for fetch_schema_details(schema_url, subject, version)."""

    @patch("src.kafka.schemas.httpx.Client")
    def test_returns_full_schema_content(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp_versions = MagicMock()
        mock_resp_versions.json.return_value = [1, 2, 3]
        mock_resp_versions.raise_for_status = MagicMock()

        mock_resp_schema = MagicMock()
        mock_resp_schema.json.return_value = {
            "version": 1,
            "id": 10,
            "schema": '{"type":"record","name":"Order","fields":[]}',
            "schemaType": "AVRO",
        }
        mock_resp_schema.raise_for_status = MagicMock()

        mock_client.get.side_effect = [mock_resp_versions, mock_resp_schema]

        result = fetch_schema_details("http://localhost:8081", "orders-value", None)
        assert result["subject"] == "orders-value"
        assert result["version"] == 1
        assert result["id"] == 10
        assert "schema" in result
        assert result["schemaType"] == "AVRO"
        assert result["allVersions"] == [1, 2, 3]

    @patch("src.kafka.schemas.httpx.Client")
    def test_with_specific_version(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_resp_versions = MagicMock()
        mock_resp_versions.json.return_value = [1, 2, 3]
        mock_resp_versions.raise_for_status = MagicMock()

        mock_resp_schema = MagicMock()
        mock_resp_schema.json.return_value = {
            "version": 2,
            "id": 20,
            "schema": '{"type":"record"}',
            "schemaType": "AVRO",
        }
        mock_resp_schema.raise_for_status = MagicMock()

        mock_client.get.side_effect = [mock_resp_versions, mock_resp_schema]

        result = fetch_schema_details("http://localhost:8081", "orders-value", "2")
        assert result["version"] == 2

    @patch("src.kafka.schemas.httpx.Client")
    def test_exception_raises_runtime_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get.side_effect = Exception("Connection refused")

        with pytest.raises(RuntimeError) as exc_info:
            fetch_schema_details("http://localhost:8081", "orders-value", "1")
        assert "Schema not found" in str(exc_info.value) or "orders-value" in str(exc_info.value)
