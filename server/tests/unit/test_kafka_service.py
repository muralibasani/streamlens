"""Unit tests for server/src/kafka/service.py."""
from unittest.mock import MagicMock, patch

import pytest

from src.kafka.service import KafkaService


@pytest.fixture
def service():
    return KafkaService()


def _cluster(**overrides):
    base = {"bootstrapServers": "localhost:9092", "id": 1}
    base.update(overrides)
    return base


class TestEmptyState:
    """Tests for _empty_state()."""

    def test_returns_dict_with_all_expected_keys_as_empty_lists(self, service):
        state = service._empty_state()
        expected_keys = ("topics", "producers", "consumers", "streams", "connectors", "schemas", "acls")
        for k in expected_keys:
            assert k in state
            assert state[k] == []


class TestCheckClusterHealth:
    """Tests for check_cluster_health(cluster)."""

    def test_no_bootstrap_servers_returns_offline(self, service):
        result = service.check_cluster_health({})
        assert result["online"] is False
        assert "No bootstrap" in result["error"]

    def test_empty_bootstrap_returns_offline(self, service):
        result = service.check_cluster_health({"bootstrapServers": "   ,  "})
        assert result["online"] is False
        assert "Invalid bootstrap" in result["error"]

    def test_invalid_bootstrap_returns_offline(self, service):
        result = service.check_cluster_health({"bootstrapServers": ""})
        assert result["online"] is False

    @patch("src.kafka.service.AdminClient")
    @patch("src.kafka.service.client_config", return_value={"bootstrap.servers": "localhost:9092"})
    def test_health_online_kraft(self, mock_cfg, mock_admin_cls, service):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_metadata = MagicMock()
        mock_metadata.topics = {"__cluster_metadata": MagicMock(), "my-topic": MagicMock()}
        mock_admin.list_topics.return_value = mock_metadata
        result = service.check_cluster_health(_cluster())
        assert result["online"] is True
        assert result["clusterMode"] == "kraft"

    @patch("src.kafka.service.AdminClient")
    @patch("src.kafka.service.client_config", return_value={"bootstrap.servers": "localhost:9092"})
    def test_health_online_zookeeper(self, mock_cfg, mock_admin_cls, service):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_metadata = MagicMock()
        mock_metadata.topics = {"my-topic": MagicMock()}  # no __cluster_metadata
        mock_admin.list_topics.return_value = mock_metadata
        result = service.check_cluster_health(_cluster())
        assert result["online"] is True
        assert result["clusterMode"] == "zookeeper"

    @patch("src.kafka.service.AdminClient")
    @patch("src.kafka.service.client_config", return_value={"bootstrap.servers": "localhost:9092"})
    def test_health_metadata_none_returns_offline(self, mock_cfg, mock_admin_cls, service):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_topics.return_value = None
        result = service.check_cluster_health(_cluster())
        assert result["online"] is False
        assert "Failed to retrieve" in result["error"]

    @patch("src.kafka.service.AdminClient")
    @patch("src.kafka.service.client_config", return_value={"bootstrap.servers": "localhost:9092"})
    def test_health_timeout_exception(self, mock_cfg, mock_admin_cls, service):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_topics.side_effect = Exception("Connection timed out")
        result = service.check_cluster_health(_cluster())
        assert result["online"] is False
        assert "Connection timeout" in result["error"]

    @patch("src.kafka.service.AdminClient")
    @patch("src.kafka.service.client_config", return_value={"bootstrap.servers": "localhost:9092"})
    def test_health_resolve_failure(self, mock_cfg, mock_admin_cls, service):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_topics.side_effect = Exception("Failed to resolve kafka.example.com")
        result = service.check_cluster_health(_cluster())
        assert result["online"] is False
        assert "Cannot resolve" in result["error"]

    @patch("src.kafka.service.AdminClient")
    @patch("src.kafka.service.client_config", return_value={"bootstrap.servers": "localhost:9092"})
    def test_health_generic_exception(self, mock_cfg, mock_admin_cls, service):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_topics.side_effect = Exception("Broker unreachable")
        result = service.check_cluster_health(_cluster())
        assert result["online"] is False
        assert result["error"] == "Broker unreachable"


class TestFetchSystemState:
    """Tests for fetch_system_state(cluster)."""

    def test_no_bootstrap_returns_empty_state(self, service):
        state = service.fetch_system_state({})
        assert state["topics"] == []
        assert state["consumers"] == []
        assert state["producers"] == []

    def test_empty_bootstrap_returns_empty_state(self, service):
        state = service.fetch_system_state({"bootstrapServers": "   "})
        assert state["topics"] == []
        assert state["consumers"] == []

    @patch("src.kafka.service.fetch_jmx_producers", return_value=[])
    @patch("src.kafka.service.fetch_acl_producers", return_value=[])
    @patch("src.kafka.service.detect_producers_by_offset_change", return_value=[])
    @patch("src.kafka.service.AdminClient")
    @patch("src.kafka.service.client_config")
    @patch("src.kafka.service.fetch_topics")
    @patch("src.kafka.service.fetch_consumer_groups")
    @patch("src.kafka.service.fetch_topic_acls")
    @patch("src.kafka.service.load_streams_config")
    @patch("src.kafka.service.fetch_connectors")
    @patch("src.kafka.service.fetch_schemas")
    def test_fetch_system_state_full(
        self, mock_schemas, mock_connectors, mock_streams, mock_acls, mock_consumers,
        mock_topics, mock_cfg, mock_admin_cls, mock_offset, mock_acl_prod, mock_jmx, service,
    ):
        mock_cfg.return_value = {"bootstrap.servers": "localhost:9092"}
        mock_topics.return_value = [{"name": "orders", "partitions": 3}]
        mock_consumers.return_value = [{"id": "group:cg1"}]
        mock_acls.return_value = []
        mock_streams.return_value = []
        mock_connectors.return_value = [{"id": "connect:my-sink"}]
        mock_schemas.return_value = [{"subject": "orders-value"}]

        cluster = _cluster(
            connectUrl="http://localhost:8083",
            schemaRegistryUrl="http://localhost:8081",
        )
        state = service.fetch_system_state(cluster)

        assert len(state["topics"]) == 1
        assert len(state["connectors"]) == 1
        assert len(state["schemas"]) == 1

    @patch("src.kafka.service.fetch_jmx_producers", return_value=[])
    @patch("src.kafka.service.fetch_acl_producers", return_value=[])
    @patch("src.kafka.service.detect_producers_by_offset_change", return_value=[])
    @patch("src.kafka.service.AdminClient")
    @patch("src.kafka.service.client_config")
    @patch("src.kafka.service.fetch_topics")
    @patch("src.kafka.service.fetch_consumer_groups")
    @patch("src.kafka.service.fetch_topic_acls")
    @patch("src.kafka.service.load_streams_config")
    def test_connect_unreachable_connectors_empty(self, mock_streams, mock_acls, mock_consumers,
                                                  mock_topics, mock_cfg, mock_admin_cls,
                                                  mock_offset, mock_acl_prod, mock_jmx, service):
        mock_cfg.return_value = {"bootstrap.servers": "localhost:9092"}
        mock_topics.return_value = []
        mock_consumers.return_value = []
        mock_acls.return_value = []
        mock_streams.return_value = []

        with patch("src.kafka.service.fetch_connectors", side_effect=Exception("Connection refused")):
            cluster = _cluster(connectUrl="http://localhost:8083")
            state = service.fetch_system_state(cluster)

        assert state["connectors"] == []

    @patch("src.kafka.service.fetch_jmx_producers", return_value=[])
    @patch("src.kafka.service.fetch_acl_producers", return_value=[])
    @patch("src.kafka.service.detect_producers_by_offset_change", return_value=[])
    @patch("src.kafka.service.AdminClient")
    @patch("src.kafka.service.client_config")
    @patch("src.kafka.service.fetch_topics")
    @patch("src.kafka.service.fetch_consumer_groups")
    @patch("src.kafka.service.fetch_topic_acls")
    @patch("src.kafka.service.load_streams_config")
    def test_schema_registry_unreachable_schemas_empty(self, mock_streams, mock_acls, mock_consumers,
                                                       mock_topics, mock_cfg, mock_admin_cls,
                                                       mock_offset, mock_acl_prod, mock_jmx, service):
        mock_cfg.return_value = {"bootstrap.servers": "localhost:9092"}
        mock_topics.return_value = []
        mock_consumers.return_value = []
        mock_acls.return_value = []
        mock_streams.return_value = []

        with patch("src.kafka.service.fetch_schemas", side_effect=Exception("Connection refused")):
            cluster = _cluster(schemaRegistryUrl="http://localhost:8081")
            state = service.fetch_system_state(cluster)

        assert state["schemas"] == []

    @patch("src.kafka.service.AdminClient")
    @patch("src.kafka.service.client_config", return_value={"bootstrap.servers": "localhost:9092"})
    def test_kafka_broker_exception_raises_runtime_error(self, mock_cfg, mock_admin_cls, service):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_topics.side_effect = Exception("Broker unreachable")

        with pytest.raises(RuntimeError) as exc_info:
            service.fetch_system_state(_cluster())
        assert "Cannot connect to Kafka" in str(exc_info.value)


class TestCollectProducers:
    """Tests for _collect_producers(cluster, admin, cfg, topics)."""

    @patch("src.kafka.service.detect_producers_by_offset_change", return_value=[])
    @patch("src.kafka.service.fetch_acl_producers", return_value=[])
    def test_no_jmx_falls_back_to_offset_detection(self, mock_acl, mock_offset, service):
        mock_admin = MagicMock()
        topics = [{"name": "orders", "partitions": 1}]
        producers = service._collect_producers(_cluster(), mock_admin, {"bootstrap.servers": "x"}, topics)
        mock_offset.assert_called_once()
        assert producers == []

    @patch("src.kafka.service.detect_producers_by_offset_change")
    @patch("src.kafka.service.fetch_acl_producers", return_value=[])
    @patch("src.kafka.service.fetch_jmx_producers", return_value=[{"id": "jmx:orders"}])
    def test_jmx_succeeds_skips_offset_detection(self, mock_jmx, mock_acl, mock_offset, service):
        mock_admin = MagicMock()
        topics = [{"name": "orders", "partitions": 1}]
        producers = service._collect_producers(
            _cluster(jmxHost="localhost", jmxPort=9999),
            mock_admin, {"bootstrap.servers": "x"}, topics,
        )
        mock_offset.assert_not_called()
        assert len(producers) == 1
        assert producers[0]["id"] == "jmx:orders"

    @patch("src.kafka.service.detect_producers_by_offset_change", return_value=[{"id": "offset:p1"}])
    @patch("src.kafka.service.fetch_acl_producers", return_value=[{"id": "acl:alice"}])
    @patch("src.kafka.service.fetch_jmx_producers", return_value=[{"id": "jmx:orders"}])
    def test_jmx_and_acl_producers_combined(self, mock_jmx, mock_acl, mock_offset, service):
        mock_admin = MagicMock()
        topics = [{"name": "orders", "partitions": 1}]
        producers = service._collect_producers(
            _cluster(jmxHost="localhost", jmxPort=9999),
            mock_admin, {"bootstrap.servers": "x"}, topics,
        )
        assert len(producers) == 2
        ids = {p["id"] for p in producers}
        assert "jmx:orders" in ids
        assert "acl:alice" in ids


class TestDelegationMethods:
    """Tests for delegation convenience methods."""

    @patch("src.kafka.service.fetch_topic_details", return_value={"name": "orders", "partitions": 3})
    def test_fetch_topic_details_calls_through(self, mock_fetch, service):
        result = service.fetch_topic_details(_cluster(), "orders", include_messages=False)
        mock_fetch.assert_called_once_with(_cluster(), "orders", False)
        assert result["name"] == "orders"

    @patch("src.kafka.service.produce_message", return_value={"ok": True, "partition": 0, "offset": 42})
    def test_produce_message_calls_through(self, mock_produce, service):
        result = service.produce_message(_cluster(), "orders", "hello", "k1")
        mock_produce.assert_called_once_with(_cluster(), "orders", "hello", "k1")
        assert result["ok"] is True

    @patch("src.kafka.service.fetch_consumer_lag", return_value={"topics": {"orders": {"partitions": []}}})
    def test_fetch_consumer_lag_calls_through(self, mock_fetch, service):
        result = service.fetch_consumer_lag(_cluster(), "my-group")
        mock_fetch.assert_called_once_with(_cluster(), "my-group")
        assert "topics" in result

    @patch("src.kafka.service.fetch_connector_details", return_value={"name": "my-sink", "config": {}})
    def test_fetch_connector_details_calls_through(self, mock_fetch, service):
        result = service.fetch_connector_details("http://localhost:8083", "my-sink")
        mock_fetch.assert_called_once_with("http://localhost:8083", "my-sink")
        assert result["name"] == "my-sink"

    @patch("src.kafka.service.fetch_schema_details", return_value={"subject": "orders-value", "version": 1})
    def test_fetch_schema_details_calls_through(self, mock_fetch, service):
        result = service.fetch_schema_details("http://localhost:8081", "orders-value", "1")
        mock_fetch.assert_called_once_with("http://localhost:8081", "orders-value", "1")
        assert result["subject"] == "orders-value"
