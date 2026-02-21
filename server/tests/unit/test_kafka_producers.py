"""Unit tests for server/src/kafka/producers.py."""
from unittest.mock import MagicMock, patch

import pytest

import src.kafka.producers as producers_mod
from src.kafka.producers import (
    _parse_active_topics,
    detect_producers_by_offset_change,
    fetch_acl_producers,
    fetch_jmx_producers,
)


@pytest.fixture(autouse=True)
def clear_offset_baseline():
    """Reset _offset_baseline between tests."""
    producers_mod._offset_baseline.clear()
    yield
    producers_mod._offset_baseline.clear()


def _make_metric(mbean_name: str, count_value: float, attribute: str = "Count"):
    """Create a mock JMX metric object."""
    m = MagicMock()
    m.mBeanName = mbean_name
    m.attribute = attribute
    m.value = count_value
    return m


class TestParseActiveTopics:
    """Tests for _parse_active_topics(metrics)."""

    def test_metric_with_topic_and_count_gt_zero_in_result(self):
        metrics = [
            _make_metric("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=orders", 42.5),
        ]
        result = _parse_active_topics(metrics)
        assert "orders" in result

    def test_metric_with_count_zero_not_in_result(self):
        metrics = [
            _make_metric("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=orders", 0),
        ]
        result = _parse_active_topics(metrics)
        assert "orders" not in result

    def test_internal_topic_filtered_out(self):
        metrics = [
            _make_metric(
                "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=__consumer_offsets", 100
            ),
        ]
        result = _parse_active_topics(metrics)
        assert "__consumer_offsets" not in result

    def test_metric_without_count_attribute_skipped(self):
        metrics = [
            _make_metric("kafka.server:type=BrokerTopicMetrics,topic=orders", 10, attribute="OneMinuteRate"),
        ]
        result = _parse_active_topics(metrics)
        assert "orders" not in result

    def test_multiple_topics_mixed(self):
        metrics = [
            _make_metric("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=orders", 5),
            _make_metric("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=events", 0),
            _make_metric(
                "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=__consumer_offsets",
                100,
            ),
        ]
        result = _parse_active_topics(metrics)
        assert result == {"orders"}

    def test_value_as_string_with_space_parsed(self):
        m = MagicMock()
        m.mBeanName = "kafka.server:...,topic=test"
        m.attribute = "Count"
        m.value = "123.45 ops"
        metrics = [m]
        result = _parse_active_topics(metrics)
        assert "test" in result


class TestFetchJmxProducers:
    """Tests for fetch_jmx_producers(jmx_host, jmx_port)."""

    @patch("jmxquery.JMXQuery", MagicMock())
    @patch("jmxquery.JMXConnection")
    def test_returns_producer_dicts_from_jmx(self, mock_jmx_conn_cls):
        mock_conn = MagicMock()
        mock_jmx_conn_cls.return_value = mock_conn

        m1 = _make_metric("kafka.server:...,topic=orders", 10)
        m2 = _make_metric("kafka.server:...,topic=events", 5)
        mock_conn.query.return_value = [m1, m2]

        result = fetch_jmx_producers("localhost", 9999)
        assert len(result) == 2
        topics = {r["producesTo"][0] for r in result}
        assert "orders" in topics
        assert "events" in topics
        assert all(r["source"] == "jmx" for r in result)

    def test_jmxquery_import_error_returns_empty(self):
        import builtins

        _orig = builtins.__import__

        def custom_import(name, *args, **kwargs):
            if name == "jmxquery":
                raise ImportError("jmxquery not installed")
            return _orig(name, *args, **kwargs)

        with patch.object(builtins, "__import__", custom_import):
            result = fetch_jmx_producers("localhost", 9999)
        assert result == []

    @patch("jmxquery.JMXQuery", MagicMock())
    @patch("jmxquery.JMXConnection")
    def test_exception_returns_empty_list(self, mock_jmx_conn_cls):
        mock_jmx_conn_cls.side_effect = Exception("Connection refused")
        result = fetch_jmx_producers("localhost", 9999)
        assert result == []


class TestFetchAclProducers:
    """Tests for fetch_acl_producers(admin)."""

    @patch("confluent_kafka.admin.AclBindingFilter", MagicMock())
    @patch("confluent_kafka.admin.ResourceType", MagicMock())
    @patch("confluent_kafka.admin.ResourcePatternType", MagicMock())
    @patch("confluent_kafka.admin.AclOperation", MagicMock())
    @patch("confluent_kafka.admin.AclPermissionType", MagicMock())
    def test_returns_grouped_producers_from_write_acls(self):
        mock_admin = MagicMock()
        acl1 = MagicMock()
        acl1.resource_name = "orders"
        acl1.principal = "User:alice"
        acl2 = MagicMock()
        acl2.resource_name = "events"
        acl2.principal = "User:alice"
        acl3 = MagicMock()
        acl3.resource_name = "logs"
        acl3.principal = "ServiceAccount:sa-1"
        mock_result = MagicMock()
        mock_result.result.return_value = [acl1, acl2, acl3]
        mock_admin.describe_acls.return_value = mock_result

        result = fetch_acl_producers(mock_admin)
        assert len(result) == 2
        principals = {r["principal"] for r in result}
        assert "alice" in principals
        assert "sa-1" in principals
        alice_producer = next(r for r in result if r["principal"] == "alice")
        assert set(alice_producer["producesTo"]) == {"orders", "events"}

    @patch("confluent_kafka.admin.AclBindingFilter", MagicMock())
    @patch("confluent_kafka.admin.ResourceType", MagicMock())
    @patch("confluent_kafka.admin.ResourcePatternType", MagicMock())
    @patch("confluent_kafka.admin.AclOperation", MagicMock())
    @patch("confluent_kafka.admin.AclPermissionType", MagicMock())
    def test_no_acls_returns_empty_list(self):
        mock_admin = MagicMock()
        mock_result = MagicMock()
        mock_result.result.return_value = []
        mock_admin.describe_acls.return_value = mock_result
        result = fetch_acl_producers(mock_admin)
        assert result == []

    @patch("confluent_kafka.admin.AclBindingFilter", MagicMock())
    @patch("confluent_kafka.admin.ResourceType", MagicMock())
    @patch("confluent_kafka.admin.ResourcePatternType", MagicMock())
    @patch("confluent_kafka.admin.AclOperation", MagicMock())
    @patch("confluent_kafka.admin.AclPermissionType", MagicMock())
    def test_filters_internal_topics(self):
        mock_admin = MagicMock()
        acl = MagicMock()
        acl.resource_name = "__consumer_offsets"
        acl.principal = "User:alice"
        mock_result = MagicMock()
        mock_result.result.return_value = [acl]
        mock_admin.describe_acls.return_value = mock_result
        result = fetch_acl_producers(mock_admin)
        assert result == []

    def test_exception_returns_empty_list(self):
        mock_admin = MagicMock()
        mock_admin.describe_acls.side_effect = Exception("ACL not enabled")
        result = fetch_acl_producers(mock_admin)
        assert result == []


class TestDetectProducersByOffsetChange:
    """Tests for detect_producers_by_offset_change(cluster_id, client_cfg, topics)."""

    @patch("src.kafka.producers.Consumer")
    def test_first_call_returns_empty_stores_baseline(self, mock_consumer_cls):
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        mock_consumer.get_watermark_offsets.return_value = (0, 100)

        topics = [{"name": "orders", "partitions": 2}]
        result = detect_producers_by_offset_change(1, {"bootstrap.servers": "localhost:9092"}, topics)

        assert result == []
        assert 1 in producers_mod._offset_baseline

    @patch("src.kafka.producers.Consumer")
    def test_second_call_with_increased_offsets_returns_producers(self, mock_consumer_cls):
        from confluent_kafka import TopicPartition

        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        def watermark_first(tp, **kwargs):
            if isinstance(tp, TopicPartition) and tp.topic == "orders":
                return (0, 100) if tp.partition == 0 else (0, 200)
            return (0, 50)

        def watermark_second(tp, **kwargs):
            if isinstance(tp, TopicPartition) and tp.topic == "orders":
                return (0, 150) if tp.partition == 0 else (0, 250)
            return (0, 50)

        mock_consumer.get_watermark_offsets.side_effect = watermark_first

        topics = [{"name": "orders", "partitions": 2}]
        client_cfg = {"bootstrap.servers": "localhost:9092"}

        first = detect_producers_by_offset_change(2, client_cfg, topics)
        assert first == []

        mock_consumer.get_watermark_offsets.side_effect = watermark_second
        second = detect_producers_by_offset_change(2, client_cfg, topics)
        assert len(second) >= 1
        assert any("orders" in p["producesTo"] for p in second)

    @patch("src.kafka.producers.Consumer")
    def test_empty_topics_returns_empty(self, mock_consumer_cls):
        result = detect_producers_by_offset_change(3, {"bootstrap.servers": "localhost:9092"}, [])
        assert result == []
