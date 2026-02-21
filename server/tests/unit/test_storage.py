"""Unit tests for server/src/storage.py."""
import json
import os

import pytest

import src.storage as storage_mod


@pytest.fixture(autouse=True)
def _setup_env(tmp_clusters_file):
    """All tests use the temporary clusters file; reload storage to pick up env."""
    import importlib

    importlib.reload(storage_mod)


class TestBoolFrom:
    """Tests for _bool_from()."""

    def test_true(self):
        assert storage_mod._bool_from(True) is True

    def test_false(self):
        assert storage_mod._bool_from(False) is False

    def test_none(self):
        assert storage_mod._bool_from(None) is False

    def test_string_true(self):
        assert storage_mod._bool_from("true") is True

    def test_string_1(self):
        assert storage_mod._bool_from("1") is True

    def test_string_yes(self):
        assert storage_mod._bool_from("yes") is True

    def test_string_false(self):
        assert storage_mod._bool_from("false") is False

    def test_string_0(self):
        assert storage_mod._bool_from("0") is False

    def test_string_no(self):
        assert storage_mod._bool_from("no") is False

    def test_string_empty(self):
        assert storage_mod._bool_from("") is False

    def test_random_string(self):
        assert storage_mod._bool_from("maybe") is False
        assert storage_mod._bool_from("random") is False


class TestResolve:
    """Tests for _resolve()."""

    def test_finds_first_non_none(self):
        c = {"a": None, "b": "value", "c": "other"}
        assert storage_mod._resolve(c, "a", "b", "c") == "value"

    def test_all_none(self):
        c = {"a": None, "b": None}
        assert storage_mod._resolve(c, "a", "b") is None

    def test_single_key_found(self):
        c = {"x": 42}
        assert storage_mod._resolve(c, "x") == 42

    def test_single_key_missing(self):
        c = {}
        assert storage_mod._resolve(c, "x") is None


class TestHasAnyKey:
    """Tests for _has_any_key()."""

    def test_matching_key(self):
        c = {"foo": 1, "bar": 2}
        assert storage_mod._has_any_key(c, "foo") is True
        assert storage_mod._has_any_key(c, "bar", "baz") is True

    def test_no_match(self):
        c = {"foo": 1}
        assert storage_mod._has_any_key(c, "bar", "baz") is False


class TestClusterFromRow:
    """Tests for _cluster_from_row()."""

    def test_minimal_cluster_dict(self):
        c = {"id": 1, "name": "test"}
        out = storage_mod._cluster_from_row(c)
        assert out["id"] == 1
        assert out["name"] == "test"
        assert out["bootstrapServers"] == ""
        assert out["createdAt"] == ""

    def test_with_ssl_fields(self):
        c = {
            "id": 2,
            "name": "ssl-cluster",
            "bootstrapServers": "broker:9092",
            "sslCaLocation": "/path/to/ca.pem",
        }
        out = storage_mod._cluster_from_row(c)
        assert out["sslCaLocation"] == "/path/to/ca.pem"

    def test_with_alias_fields_snake_case(self):
        c = {
            "id": 3,
            "name": "alias-cluster",
            "bootstrap_servers": "localhost:9092",
            "schema_registry_url": "http://localhost:8081",
            "created_at": "2025-01-15T12:00:00Z",
        }
        out = storage_mod._cluster_from_row(c)
        assert out["bootstrapServers"] == "localhost:9092"
        assert out["schemaRegistryUrl"] == "http://localhost:8081"
        assert out["createdAt"] == "2025-01-15T12:00:00Z"


class TestStripUrlCredentials:
    """Tests for _strip_url_credentials()."""

    def test_with_credentials(self):
        url = "http://user:pass@localhost:8081/schemas"
        result = storage_mod._strip_url_credentials(url)
        assert "user" not in result
        assert "pass" not in result
        assert "localhost:8081" in result

    def test_without_credentials(self):
        url = "http://localhost:8081/schemas"
        result = storage_mod._strip_url_credentials(url)
        assert result == url

    def test_none(self):
        assert storage_mod._strip_url_credentials(None) is None

    def test_empty(self):
        assert storage_mod._strip_url_credentials("") == ""

    def test_malformed_url(self):
        """Malformed URL should return as-is (exception caught)."""
        url = "not-a-valid-url://"
        result = storage_mod._strip_url_credentials(url)
        assert result == url


class TestUpdateCluster:
    """Tests for update_cluster()."""

    def test_updates_name(self, tmp_clusters_file):
        result = storage_mod.update_cluster(
            1,
            name="updated-name",
            bootstrap_servers="localhost:9092",
        )
        assert result is not None
        assert result["name"] == "updated-name"

    def test_updates_bootstrap_servers(self, tmp_clusters_file):
        result = storage_mod.update_cluster(
            1,
            name="test-cluster",
            bootstrap_servers="broker:9093",
        )
        assert result is not None
        assert result["bootstrapServers"] == "broker:9093"

    def test_preserves_created_at(self, tmp_clusters_file):
        """createdAt from existing cluster must be preserved."""
        original = json.loads(tmp_clusters_file.read_text())
        orig_created = original["clusters"][0]["createdAt"]

        storage_mod.update_cluster(
            1,
            name="test-cluster",
            bootstrap_servers="localhost:9092",
        )

        updated = storage_mod.get_cluster(1)
        assert updated["createdAt"] == orig_created

    def test_not_found_returns_none(self, tmp_clusters_file):
        result = storage_mod.update_cluster(
            999,
            name="nope",
            bootstrap_servers="localhost:9092",
        )
        assert result is None


class TestSnapshot:
    """Tests for get_latest_snapshot() and create_snapshot()."""

    def test_create_then_retrieve(self):
        cluster_id = 1
        data = {"nodes": [{"id": "topic:t1"}], "edges": []}

        before = storage_mod.get_latest_snapshot(cluster_id)
        assert before is None

        created = storage_mod.create_snapshot(cluster_id, data)
        assert created["clusterId"] == cluster_id
        assert created["data"] == data
        assert "createdAt" in created

        retrieved = storage_mod.get_latest_snapshot(cluster_id)
        assert retrieved is not None
        assert retrieved["data"] == data
