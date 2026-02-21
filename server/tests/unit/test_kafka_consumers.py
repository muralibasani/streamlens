"""Unit tests for server/src/kafka/consumers.py."""
from unittest.mock import MagicMock, patch

import pytest

from src.kafka.consumers import (
    _discover_topics_from_members,
    _discover_topics_from_offsets,
    _is_likely_streams_app,
    _query_watermarks_for_lag,
    fetch_consumer_groups,
    fetch_consumer_lag,
)


class TestIsLikelyStreamsApp:
    """Tests for _is_likely_streams_app(group_id)."""

    def test_my_stream_app_returns_true(self):
        assert _is_likely_streams_app("my-stream-app") is True

    def test_processor_service_returns_true(self):
        assert _is_likely_streams_app("processor-service") is True

    def test_my_consumer_returns_false(self):
        assert _is_likely_streams_app("my-consumer") is False

    def test_aggregator_returns_true(self):
        assert _is_likely_streams_app("aggregator") is True

    def test_kstream_app_returns_true(self):
        assert _is_likely_streams_app("my-kstream-app") is True

    def test_transformer_returns_true(self):
        assert _is_likely_streams_app("transformer-service") is True

    def test_enricher_returns_true(self):
        assert _is_likely_streams_app("enricher") is True

    def test_application_suffix_returns_true(self):
        assert _is_likely_streams_app("my-application") is True

    def test_plain_consumer_group_returns_false(self):
        assert _is_likely_streams_app("orders-consumer-group") is False


class TestQueryWatermarksForLag:
    """Tests for _query_watermarks_for_lag(client_cfg, tp_list)."""

    @patch("src.kafka.consumers.Consumer")
    def test_returns_computed_lag_from_watermarks(self, mock_consumer_cls):
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        def watermark_side_effect(tp, **kwargs):
            if tp.topic == "orders" and tp.partition == 0:
                return (0, 100)
            return (0, 50)

        mock_consumer.get_watermark_offsets.side_effect = watermark_side_effect

        tp_list = [("orders", 0, 90), ("events", 0, 30)]
        result = _query_watermarks_for_lag({"bootstrap.servers": "localhost:9092"}, tp_list)

        assert "orders" in result
        assert "events" in result
        assert len(result["orders"]["partitions"]) == 1
        assert result["orders"]["partitions"][0]["currentOffset"] == 90
        assert result["orders"]["partitions"][0]["logEndOffset"] == 100
        assert result["orders"]["partitions"][0]["lag"] == 10

    @patch("src.kafka.consumers.Consumer")
    def test_empty_tp_list_returns_empty_dict(self, mock_consumer_cls):
        result = _query_watermarks_for_lag({"bootstrap.servers": "localhost:9092"}, [])
        assert result == {}

    @patch("src.kafka.consumers.Consumer")
    def test_watermark_query_failure_handled_gracefully(self, mock_consumer_cls):
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        mock_consumer.get_watermark_offsets.side_effect = Exception("Broker unreachable")

        tp_list = [("orders", 0, 10)]
        result = _query_watermarks_for_lag({"bootstrap.servers": "localhost:9092"}, tp_list)
        assert "orders" in result
        assert result["orders"]["partitions"][0]["lag"] == 0


class TestFetchConsumerGroups:
    """Tests for fetch_consumer_groups(admin, client_cfg)."""

    def test_returns_consumer_list_with_topics(self):
        mock_admin = MagicMock()

        mock_group = MagicMock()
        mock_group.id = "my-consumer-group"
        mock_group.protocol_type = "consumer"

        mock_member = MagicMock()
        mock_member.metadata = b'{"topic_partitions":[{"topic":"orders"}]}'
        mock_member.assignment = b""
        mock_group.members = [mock_member]

        mock_admin.list_groups.return_value = [mock_group]

        mock_metadata = MagicMock()
        mock_metadata.topics = {"orders": MagicMock(), "events": MagicMock()}
        mock_admin.list_topics.return_value = mock_metadata

        result = fetch_consumer_groups(mock_admin, {"bootstrap.servers": "localhost:9092"})

        assert len(result) >= 1
        assert any(c["id"] == "group:my-consumer-group" for c in result)

    def test_no_groups_returns_empty_list(self):
        mock_admin = MagicMock()
        mock_admin.list_groups.return_value = []

        result = fetch_consumer_groups(mock_admin, {"bootstrap.servers": "localhost:9092"})
        assert result == []

    def test_filters_non_consumer_protocol_types(self):
        mock_admin = MagicMock()

        consumer_group = MagicMock()
        consumer_group.id = "consumer-1"
        consumer_group.protocol_type = "consumer"

        connect_group = MagicMock()
        connect_group.id = "connect-1"
        connect_group.protocol_type = "connector"

        mock_admin.list_groups.return_value = [consumer_group, connect_group]

        mock_metadata = MagicMock()
        mock_metadata.topics = {}
        mock_admin.list_topics.return_value = mock_metadata

        with patch("src.kafka.consumers._discover_topics_from_offsets") as mock_offsets:
            mock_offsets.return_value = set()
            result = fetch_consumer_groups(mock_admin, {"bootstrap.servers": "localhost:9092"})

        assert len(result) == 1
        assert result[0]["id"] == "group:consumer-1"


class TestFetchConsumerLag:
    """Tests for fetch_consumer_lag(cluster, group_id)."""

    @patch("src.kafka.consumers.AdminClient")
    @patch("src.kafka.consumers.client_config", return_value={"bootstrap.servers": "localhost:9092"})
    def test_returns_lag_data(self, mock_cfg, mock_admin_cls):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin

        mock_tp = MagicMock()
        mock_tp.topic = "orders"
        mock_tp.partition = 0
        mock_tp.offset = 90

        mock_gtp = MagicMock()
        mock_gtp.topic_partitions = [mock_tp]

        mock_future = MagicMock()
        mock_future.result.return_value = mock_gtp

        mock_admin.list_consumer_group_offsets.return_value = {
            ("my-group",): mock_future,
        }

        with patch("src.kafka.consumers._query_watermarks_for_lag") as mock_query:
            mock_query.return_value = {
                "orders": {
                    "partitions": [
                        {"partition": 0, "currentOffset": 90, "logEndOffset": 100, "lag": 10},
                    ],
                },
            }
            result = fetch_consumer_lag({"bootstrapServers": "localhost:9092"}, "my-group")

        assert "orders" in result["topics"]
        assert result["topics"]["orders"]["partitions"][0]["lag"] == 10

    @patch("src.kafka.consumers.AdminClient")
    @patch("src.kafka.consumers.client_config", return_value={"bootstrap.servers": "localhost:9092"})
    def test_empty_metadata_returns_empty_topics(self, mock_cfg, mock_admin_cls):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_consumer_group_offsets.return_value = {}

        result = fetch_consumer_lag({"bootstrapServers": "localhost:9092"}, "my-group")

        assert result["topics"] == {}

    @patch("src.kafka.consumers.AdminClient")
    @patch("src.kafka.consumers.client_config", return_value={"bootstrap.servers": "localhost:9092"})
    def test_exception_raises_runtime_error(self, mock_cfg, mock_admin_cls):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_consumer_group_offsets.side_effect = Exception("Broker unreachable")

        with pytest.raises(RuntimeError) as exc_info:
            fetch_consumer_lag({"bootstrapServers": "localhost:9092"}, "my-group")
        assert "Could not fetch consumer lag" in str(exc_info.value)


class TestDiscoverTopicsFromMembers:
    """Tests for _discover_topics_from_members(admin, group_metadata)."""

    def test_extracts_topics_from_member_metadata(self):
        mock_admin = MagicMock()
        mock_admin.list_topics.return_value = MagicMock(topics={"orders": MagicMock(), "events": MagicMock()})

        mock_member = MagicMock()
        mock_member.metadata = b'{"topic_partitions":[{"topic":"orders"}]}'
        mock_member.assignment = b""

        mock_group = MagicMock()
        mock_group.members = [mock_member]

        result = _discover_topics_from_members(mock_admin, mock_group)

        assert "orders" in result

    def test_extracts_topics_from_member_assignment(self):
        mock_admin = MagicMock()
        mock_admin.list_topics.return_value = MagicMock(topics={"events": MagicMock()})

        mock_member = MagicMock()
        mock_member.metadata = b""
        mock_member.assignment = b'{"partitions":[{"topic":"events"}]}'

        mock_group = MagicMock()
        mock_group.members = [mock_member]

        result = _discover_topics_from_members(mock_admin, mock_group)

        assert "events" in result

    def test_no_members_returns_empty(self):
        mock_admin = MagicMock()
        mock_group = MagicMock()
        mock_group.members = []

        result = _discover_topics_from_members(mock_admin, mock_group)

        assert result == set()

    def test_none_group_returns_empty(self):
        mock_admin = MagicMock()
        result = _discover_topics_from_members(mock_admin, None)
        assert result == set()


class TestDiscoverTopicsFromOffsets:
    """Tests for _discover_topics_from_offsets(client_cfg, group_id, admin)."""

    @patch("src.kafka.consumers.Consumer")
    def test_discovers_topics_with_committed_offsets(self, mock_consumer_cls):
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        mock_consumer.list_topics.return_value = MagicMock(
            topics={
                "orders": MagicMock(partitions={0: MagicMock()}),
                "__internal": MagicMock(partitions={0: MagicMock()}),
            }
        )

        def committed_side_effect(tps, timeout=None):
            for tp in tps:
                tp.offset = 10 if tp.topic == "orders" else -1001
            return tps

        mock_consumer.committed.side_effect = committed_side_effect

        result = _discover_topics_from_offsets(
            {"bootstrap.servers": "localhost:9092"},
            "my-group",
            MagicMock(),
        )

        assert "orders" in result
        assert "__internal" not in result

    @patch("src.kafka.consumers.Consumer")
    def test_no_committed_offsets_returns_empty(self, mock_consumer_cls):
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        mock_consumer.list_topics.return_value = MagicMock(
            topics={"orders": MagicMock(partitions={0: MagicMock()})}
        )

        def committed_side_effect(tps, timeout=None):
            for tp in tps:
                tp.offset = -1001
            return tps

        mock_consumer.committed.side_effect = committed_side_effect

        result = _discover_topics_from_offsets(
            {"bootstrap.servers": "localhost:9092"},
            "my-group",
            MagicMock(),
        )

        assert result == set()

    @patch("src.kafka.consumers.Consumer")
    def test_exception_returns_empty(self, mock_consumer_cls):
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        mock_consumer.list_topics.side_effect = Exception("Broker unreachable")

        result = _discover_topics_from_offsets(
            {"bootstrap.servers": "localhost:9092"},
            "my-group",
            MagicMock(),
        )

        assert result == set()
