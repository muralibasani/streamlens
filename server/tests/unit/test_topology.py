"""Unit tests for server/src/topology.py — build_topology, paginate, search."""
from unittest.mock import patch, MagicMock

import pytest

from src.topology import build_topology, paginate_topology_data, search_topology


def _fake_state(**overrides):
    """Return a minimal system state dict with optional overrides."""
    base = {
        "topics": [{"name": "orders", "partitions": 3, "replication": 1}],
        "producers": [],
        "consumers": [],
        "streams": [],
        "connectors": [],
        "schemas": [],
        "acls": [],
    }
    base.update(overrides)
    return base


class TestBuildTopology:
    @patch("src.topology.kafka_service")
    def test_basic_topic_node(self, mock_ks):
        mock_ks.fetch_system_state.return_value = _fake_state()
        result = build_topology(1, {"bootstrapServers": "localhost:9092"})

        topic_nodes = [n for n in result["nodes"] if n["type"] == "topic"]
        assert len(topic_nodes) == 1
        assert topic_nodes[0]["id"] == "topic:orders"
        assert topic_nodes[0]["data"]["label"] == "orders"

    @patch("src.topology.kafka_service")
    def test_consumer_creates_node_and_edge(self, mock_ks):
        mock_ks.fetch_system_state.return_value = _fake_state(
            consumers=[{"id": "group:my-group", "consumesFrom": ["orders"], "source": "auto-discovered", "isStreams": False}]
        )
        result = build_topology(1, {})

        consumer_nodes = [n for n in result["nodes"] if n["type"] == "consumer"]
        assert len(consumer_nodes) == 1
        assert consumer_nodes[0]["data"]["label"] == "my-group"

        consume_edges = [e for e in result["edges"] if e["type"] == "consumes"]
        assert len(consume_edges) == 1
        assert consume_edges[0]["source"] == "topic:orders"

    @patch("src.topology.kafka_service")
    def test_producer_creates_node_and_edge(self, mock_ks):
        mock_ks.fetch_system_state.return_value = _fake_state(
            producers=[{"id": "acl:my-app", "producesTo": ["orders"], "source": "acl", "principal": "my-app"}]
        )
        result = build_topology(1, {})

        producer_nodes = [n for n in result["nodes"] if n["type"] == "producer"]
        assert len(producer_nodes) == 1

        produce_edges = [e for e in result["edges"] if e["type"] == "produces"]
        assert len(produce_edges) == 1
        assert produce_edges[0]["target"] == "topic:orders"

    @patch("src.topology.kafka_service")
    def test_schema_single_subject_uses_subject_as_label(self, mock_ks):
        mock_ks.fetch_system_state.return_value = _fake_state(
            schemas=[{"subject": "orders-value", "version": 1, "id": 10, "type": "AVRO", "topicName": "orders"}]
        )
        result = build_topology(1, {})

        schema_nodes = [n for n in result["nodes"] if n["type"] == "schema"]
        assert len(schema_nodes) == 1
        assert schema_nodes[0]["data"]["label"] == "orders-value"
        assert schema_nodes[0]["data"]["subjects"] == ["orders-value"]

    @patch("src.topology.kafka_service")
    def test_schemas_same_id_grouped_into_one_node(self, mock_ks):
        mock_ks.fetch_system_state.return_value = _fake_state(
            topics=[
                {"name": "orders", "partitions": 3, "replication": 1},
                {"name": "payments", "partitions": 2, "replication": 1},
            ],
            schemas=[
                {"subject": "orders-value", "version": 1, "id": 42, "type": "AVRO", "topicName": "orders"},
                {"subject": "payments-value", "version": 1, "id": 42, "type": "AVRO", "topicName": "payments"},
            ],
        )
        result = build_topology(1, {})

        schema_nodes = [n for n in result["nodes"] if n["type"] == "schema"]
        assert len(schema_nodes) == 1
        assert schema_nodes[0]["data"]["label"] == "Multiple subjects"
        assert set(schema_nodes[0]["data"]["subjects"]) == {"orders-value", "payments-value"}

        schema_edges = [e for e in result["edges"] if e["type"] == "schema_link"]
        assert len(schema_edges) == 2
        edge_sources = {e["source"] for e in schema_edges}
        assert edge_sources == {"topic:orders", "topic:payments"}

    @patch("src.topology.kafka_service")
    def test_schemas_different_ids_separate_nodes(self, mock_ks):
        mock_ks.fetch_system_state.return_value = _fake_state(
            schemas=[
                {"subject": "orders-value", "version": 1, "id": 10, "type": "AVRO", "topicName": "orders"},
                {"subject": "orders-key", "version": 1, "id": 11, "type": "AVRO", "topicName": "orders"},
            ],
        )
        result = build_topology(1, {})

        schema_nodes = [n for n in result["nodes"] if n["type"] == "schema"]
        assert len(schema_nodes) == 2

    @patch("src.topology.kafka_service")
    def test_schema_without_matching_topic_skipped(self, mock_ks):
        mock_ks.fetch_system_state.return_value = _fake_state(
            schemas=[{"subject": "ghost-value", "version": 1, "id": 99, "type": "AVRO", "topicName": "ghost"}]
        )
        result = build_topology(1, {})

        schema_nodes = [n for n in result["nodes"] if n["type"] == "schema"]
        assert len(schema_nodes) == 0

    @patch("src.topology.kafka_service")
    def test_connector_nodes_and_edges(self, mock_ks):
        mock_ks.fetch_system_state.return_value = _fake_state(
            connectors=[{"id": "connect:my-sink", "type": "sink", "topic": "orders"}]
        )
        result = build_topology(1, {})

        connector_nodes = [n for n in result["nodes"] if n["type"] == "connector"]
        assert len(connector_nodes) == 1

        edges = [e for e in result["edges"] if e["type"] == "sinks"]
        assert len(edges) == 1
        assert edges[0]["source"] == "topic:orders"

    @patch("src.topology.kafka_service")
    def test_streams_creates_topic_to_topic_edge(self, mock_ks):
        mock_ks.fetch_system_state.return_value = _fake_state(
            topics=[
                {"name": "input", "partitions": 1, "replication": 1},
                {"name": "output", "partitions": 1, "replication": 1},
            ],
            streams=[{"id": "streams:my-app", "label": "my-app", "name": "my-app",
                       "consumerGroup": "my-app", "consumesFrom": ["input"], "producesTo": ["output"], "source": "config"}],
        )
        result = build_topology(1, {})

        streams_edges = [e for e in result["edges"] if e["type"] == "streams"]
        assert len(streams_edges) == 1
        assert streams_edges[0]["source"] == "topic:input"
        assert streams_edges[0]["target"] == "topic:output"
        assert streams_edges[0]["label"] == "my-app"

    @patch("src.topology.kafka_service")
    def test_acl_nodes(self, mock_ks):
        mock_ks.fetch_system_state.return_value = _fake_state(
            acls=[
                {"topic": "orders", "principal": "User:alice", "host": "*", "operation": "READ", "permissionType": "ALLOW"},
                {"topic": "orders", "principal": "User:bob", "host": "*", "operation": "WRITE", "permissionType": "ALLOW"},
            ]
        )
        result = build_topology(1, {})

        acl_nodes = [n for n in result["nodes"] if n["type"] == "acl"]
        assert len(acl_nodes) == 1
        assert acl_nodes[0]["data"]["label"] == "ACL (2)"
        assert len(acl_nodes[0]["data"]["acls"]) == 2

    @patch("src.topology.kafka_service")
    def test_build_topology_exception_returns_empty(self, mock_ks):
        """When fetch_system_state raises, build_topology returns empty nodes and edges."""
        mock_ks.fetch_system_state.side_effect = Exception("connection failed")
        result = build_topology(1, {"bootstrapServers": "localhost:9092"})
        assert result["nodes"] == []
        assert result["edges"] == []

    @patch("src.topology.kafka_service")
    def test_build_topology_more_topics_than_max_sampling(self, mock_ks):
        """When cluster has more topics than max_topics, sampling is applied and _meta is set."""
        import os

        mock_ks.fetch_system_state.return_value = _fake_state(
            topics=[{"name": f"topic-{i}", "partitions": 1, "replication": 1} for i in range(50)]
        )
        with patch.dict(os.environ, {"TOPOLOGY_MAX_TOPICS": "10"}):
            result = build_topology(1, {"bootstrapServers": "localhost:9092"})

        topic_nodes = [n for n in result["nodes"] if n["type"] == "topic"]
        assert len(topic_nodes) <= 10
        assert "_meta" in result
        assert result["_meta"]["totalTopicCount"] == 50
        assert result["_meta"]["shownTopicCount"] <= 10

    @patch("src.topology.kafka_service")
    def test_build_topology_sampling_includes_connected_topics(self, mock_ks):
        """Sampling should always include connected topics (consumers/producers)."""
        import os

        mock_ks.fetch_system_state.return_value = _fake_state(
            topics=[{"name": f"topic-{i}", "partitions": 1, "replication": 1} for i in range(100)],
            consumers=[{"id": "group:cg", "consumesFrom": ["topic-0", "topic-42"], "source": "auto", "isStreams": False}],
        )
        with patch.dict(os.environ, {"TOPOLOGY_MAX_TOPICS": "20"}):
            result = build_topology(1, {"bootstrapServers": "localhost:9092"})

        topic_ids = {n["id"] for n in result["nodes"] if n["type"] == "topic"}
        assert "topic:topic-0" in topic_ids
        assert "topic:topic-42" in topic_ids
        assert "_meta" in result
        assert result["_meta"]["totalTopicCount"] == 100


class TestPaginateTopology:
    def _make_topology(self, topic_count, with_consumer=False):
        nodes = [{"id": f"topic:t{i}", "type": "topic", "data": {"label": f"t{i}"}} for i in range(topic_count)]
        edges = []
        if with_consumer:
            nodes.append({"id": "group:cg", "type": "consumer", "data": {"label": "cg"}})
            edges.append({"id": "t0->cg", "source": "topic:t0", "target": "group:cg", "type": "consumes"})
        return {"nodes": nodes, "edges": edges}

    def test_returns_first_page(self):
        data = self._make_topology(10)
        page = paginate_topology_data(data, offset=0, limit=3)

        topic_nodes = [n for n in page["nodes"] if n["type"] == "topic"]
        assert len(topic_nodes) == 3
        assert page["_meta"]["hasMore"] is True
        assert page["_meta"]["totalTopicCount"] == 10

    def test_connected_topics_come_first(self):
        data = self._make_topology(5, with_consumer=True)
        page = paginate_topology_data(data, offset=0, limit=2)

        topic_nodes = [n for n in page["nodes"] if n["type"] == "topic"]
        assert topic_nodes[0]["id"] == "topic:t0"

    def test_last_page_has_no_more(self):
        data = self._make_topology(3)
        page = paginate_topology_data(data, offset=0, limit=10)
        assert page["_meta"]["hasMore"] is False

    def test_non_topic_nodes_included_when_connected(self):
        data = self._make_topology(5, with_consumer=True)
        page = paginate_topology_data(data, offset=0, limit=5)

        consumer_nodes = [n for n in page["nodes"] if n["type"] == "consumer"]
        assert len(consumer_nodes) == 1


class TestSearchTopology:
    def _make_topology(self):
        return {
            "nodes": [
                {"id": "topic:orders", "type": "topic", "data": {"label": "orders"}},
                {"id": "topic:payments", "type": "topic", "data": {"label": "payments"}},
                {"id": "group:my-group", "type": "consumer", "data": {"label": "my-group"}},
            ],
            "edges": [
                {"id": "orders->cg", "source": "topic:orders", "target": "group:my-group", "type": "consumes"},
            ],
        }

    def test_search_by_label(self):
        result = search_topology(self._make_topology(), "orders")
        assert "topic:orders" in result["matchIds"]

    def test_search_includes_connected_nodes(self):
        result = search_topology(self._make_topology(), "orders")
        node_ids = {n["id"] for n in result["nodes"]}
        assert "group:my-group" in node_ids

    def test_search_no_match(self):
        result = search_topology(self._make_topology(), "nonexistent")
        assert result["matchIds"] == []
        assert result["nodes"] == []

    def test_search_empty_query(self):
        result = search_topology(self._make_topology(), "")
        assert result["matchIds"] == []

    def test_search_by_type(self):
        result = search_topology(self._make_topology(), "consumer")
        assert "group:my-group" in result["matchIds"]
