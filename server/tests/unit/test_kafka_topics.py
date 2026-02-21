"""Unit tests for server/src/kafka/topics.py."""
from unittest.mock import MagicMock, patch

import pytest

from src.kafka.topics import (
    _fetch_recent_messages,
    _format_retention,
    fetch_topic_details,
    fetch_topics,
    produce_message,
)


class TestFormatRetention:
    """Tests for _format_retention(retention_ms)."""

    def test_na_returns_na(self):
        assert _format_retention("N/A") == "N/A"

    def test_minus_one_returns_unlimited(self):
        assert _format_retention("-1") == "Unlimited"

    def test_seven_days_returns_7d_0h(self):
        assert _format_retention("604800000") == "7d 0h"

    def test_one_day_one_hour_returns_1d_1h(self):
        assert _format_retention("90000000") == "1d 1h"

    def test_one_hour_returns_1h(self):
        assert _format_retention("3600000") == "1h"

    def test_invalid_value_returns_as_string(self):
        assert _format_retention("invalid") == "invalid"

    def test_two_days(self):
        assert _format_retention("172800000") == "2d 0h"

    def test_zero_days_returns_hours_only(self):
        assert _format_retention("7200000") == "2h"


class TestFetchTopics:
    """Tests for fetch_topics(admin)."""

    def test_returns_topic_list_from_metadata(self):
        mock_admin = MagicMock()
        mock_topic = MagicMock()
        mock_topic.partitions = {0: MagicMock(replicas=[1, 2, 3]), 1: MagicMock(replicas=[1, 2, 3])}
        mock_metadata = MagicMock()
        mock_metadata.topics = {
            "orders": mock_topic,
            "events": MagicMock(partitions={0: MagicMock(replicas=[1, 2])}),
        }
        mock_admin.list_topics.return_value = mock_metadata

        result = fetch_topics(mock_admin)
        assert len(result) == 2
        names = {t["name"] for t in result}
        assert "orders" in names
        assert "events" in names

    def test_internal_topics_filtered(self):
        mock_admin = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topics = {
            "__consumer_offsets": MagicMock(partitions={0: MagicMock(replicas=[1])}),
            "orders": MagicMock(partitions={0: MagicMock(replicas=[1, 2])}),
        }
        mock_admin.list_topics.return_value = mock_metadata

        result = fetch_topics(mock_admin)
        assert len(result) == 1
        assert result[0]["name"] == "orders"

    def test_exception_propagates(self):
        mock_admin = MagicMock()
        mock_admin.list_topics.side_effect = Exception("Broker unreachable")
        with pytest.raises(Exception):
            fetch_topics(mock_admin)


class TestProduceMessage:
    """Tests for produce_message(cluster, topic_name, value, key)."""

    def test_internal_topic_raises_runtime_error(self):
        cluster = {"bootstrapServers": "localhost:9092"}
        with pytest.raises(RuntimeError) as exc_info:
            produce_message(cluster, "__consumer_offsets", "value", "key")
        assert "internal" in str(exc_info.value).lower() or "Cannot produce" in str(exc_info.value)

    def test_empty_topic_raises_runtime_error(self):
        cluster = {"bootstrapServers": "localhost:9092"}
        with pytest.raises(RuntimeError) as exc_info:
            produce_message(cluster, "", "value", "key")
        assert "internal" in str(exc_info.value).lower() or "Cannot produce" in str(exc_info.value)

    def test_no_bootstrap_servers_raises_runtime_error(self):
        cluster = {}
        with pytest.raises(RuntimeError) as exc_info:
            produce_message(cluster, "orders", "value", "key")
        assert "bootstrap" in str(exc_info.value).lower() or "No bootstrap" in str(exc_info.value)

    @patch("src.kafka.topics.Producer")
    def test_successful_produce_returns_result(self, mock_producer_cls):
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        def capture_callback(err, msg):
            if err is None:
                m = MagicMock()
                m.partition.return_value = 0
                m.offset.return_value = 42
                capture_callback.msg = m
            else:
                capture_callback.err = err

        capture_callback.msg = None
        capture_callback.err = None

        def produce_side_effect(topic, value=None, key=None, callback=None):
            callback(None, MagicMock(partition=MagicMock(return_value=0), offset=MagicMock(return_value=42)))

        mock_producer.produce.side_effect = produce_side_effect

        cluster = {"bootstrapServers": "localhost:9092"}
        result = produce_message(cluster, "orders", "hello", "k1")
        assert result["ok"] is True
        assert result["partition"] == 0
        assert result["offset"] == 42


class TestFetchTopicDetails:
    """Tests for fetch_topic_details(cluster, topic_name, include_messages)."""

    @patch("src.kafka.topics.client_config", return_value={"bootstrap.servers": "localhost:9092"})
    @patch("src.kafka.topics.AdminClient")
    def test_returns_full_details(self, mock_admin_cls, mock_cfg):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin

        config_entry = MagicMock()
        config_entry.name = "retention.ms"
        config_entry.value = "604800000"
        mock_config_future = MagicMock()
        mock_config_future.result.return_value = {"retention.ms": config_entry}
        mock_admin.describe_configs.return_value = {MagicMock(): mock_config_future}

        mock_topic = MagicMock()
        mock_topic.partitions = {0: MagicMock(replicas=[1, 2])}
        mock_metadata = MagicMock()
        mock_metadata.topics = {"orders": mock_topic}
        mock_admin.list_topics.return_value = mock_metadata

        result = fetch_topic_details({"bootstrapServers": "localhost:9092"}, "orders")

        assert result["name"] == "orders"
        assert result["partitions"] == 1
        assert result["replicationFactor"] == 2
        assert result["config"]["retentionMs"] == "604800000"
        assert result["recentMessages"] == []

    @patch("src.kafka.topics._fetch_recent_messages", return_value=[{"partition": 0, "offset": 1, "key": "k1", "value": "v1"}])
    @patch("src.kafka.topics.client_config", return_value={"bootstrap.servers": "localhost:9092"})
    @patch("src.kafka.topics.AdminClient")
    def test_include_messages_calls_fetch_recent_messages(self, mock_admin_cls, mock_cfg, mock_fetch):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin

        config_entry = MagicMock()
        config_entry.name = "retention.ms"
        config_entry.value = "604800000"
        mock_config_future = MagicMock()
        mock_config_future.result.return_value = {"retention.ms": config_entry}
        mock_admin.describe_configs.return_value = {MagicMock(): mock_config_future}

        mock_topic = MagicMock()
        mock_topic.partitions = {0: MagicMock(replicas=[1, 2])}
        mock_metadata = MagicMock()
        mock_metadata.topics = {"orders": mock_topic}
        mock_admin.list_topics.return_value = mock_metadata

        result = fetch_topic_details(
            {"bootstrapServers": "localhost:9092"}, "orders", include_messages=True
        )

        mock_fetch.assert_called_once()
        assert len(result["recentMessages"]) == 1
        assert result["recentMessages"][0]["value"] == "v1"

    @patch("src.kafka.topics.client_config", return_value={"bootstrap.servers": "localhost:9092"})
    @patch("src.kafka.topics.AdminClient")
    def test_exception_raises_runtime_error(self, mock_admin_cls, mock_cfg):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.describe_configs.side_effect = Exception("Broker unreachable")

        with pytest.raises(RuntimeError) as exc_info:
            fetch_topic_details({"bootstrapServers": "localhost:9092"}, "orders")
        assert "Could not fetch topic details" in str(exc_info.value)


class TestFetchRecentMessages:
    """Tests for _fetch_recent_messages(cfg, topic_name, partitions_count)."""

    @patch("src.kafka.topics.Consumer")
    def test_returns_message_list(self, mock_consumer_cls):
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        mock_consumer.get_watermark_offsets.return_value = (0, 10)

        mock_msg = MagicMock()
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 5
        mock_msg.key.return_value = b"k1"
        mock_msg.value.return_value = b"v1"
        mock_msg.timestamp.return_value = (1, 1234567890)
        mock_msg.error.return_value = None

        mock_consumer.poll.side_effect = [mock_msg, None, None]

        result = _fetch_recent_messages({"bootstrap.servers": "localhost:9092"}, "orders", 1)

        assert len(result) >= 1
        assert result[0]["partition"] == 0
        assert result[0]["offset"] == 5
        assert result[0]["key"] == "k1"
        assert result[0]["value"] == "v1"

    @patch("src.kafka.topics.time")
    @patch("src.kafka.topics.Consumer")
    def test_no_messages_returns_empty_list(self, mock_consumer_cls, mock_time):
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        mock_consumer.get_watermark_offsets.return_value = (0, 0)
        mock_consumer.poll.return_value = None
        mock_time.time.side_effect = [0, 4]

        result = _fetch_recent_messages({"bootstrap.servers": "localhost:9092"}, "orders", 1)

        assert result == []

    @patch("src.kafka.topics.Consumer")
    def test_exception_returns_empty_list(self, mock_consumer_cls):
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        mock_consumer.get_watermark_offsets.side_effect = Exception("Broker unreachable")

        result = _fetch_recent_messages({"bootstrap.servers": "localhost:9092"}, "orders", 1)

        assert result == []
