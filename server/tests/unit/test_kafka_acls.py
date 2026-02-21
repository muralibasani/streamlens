"""Unit tests for server/src/kafka/acls.py."""
from unittest.mock import MagicMock, patch

import pytest

from src.kafka.acls import _parse_binding, fetch_topic_acls


def _mock_acl(
    name="orders",
    principal="User:alice",
    host="*",
    operation_name="READ",
    perm_name="ALLOW",
):
    """Create a mock ACL binding object."""
    acl = MagicMock()
    acl.name = name
    acl.resource_name = name
    acl.principal = principal
    acl.host = host
    op = MagicMock()
    op.name = operation_name
    acl.operation = op
    perm = MagicMock()
    perm.name = perm_name
    acl.permission_type = perm
    return acl


class TestParseBinding:
    """Tests for _parse_binding(acl)."""

    def test_valid_acl_binding_returns_dict(self):
        acl = _mock_acl(
            name="orders",
            principal="User:alice",
            host="*",
            operation_name="READ",
            perm_name="ALLOW",
        )
        result = _parse_binding(acl)
        assert result is not None
        assert result["topic"] == "orders"
        assert result["principal"] == "User:alice"
        assert result["host"] == "*"
        assert result["operation"] == "READ"
        assert result["permissionType"] == "ALLOW"

    def test_internal_topic_returns_none(self):
        acl = _mock_acl(name="__consumer_offsets")
        result = _parse_binding(acl)
        assert result is None

    def test_internal_topic_double_underscore_returns_none(self):
        acl = _mock_acl(name="__internal_topic")
        result = _parse_binding(acl)
        assert result is None

    def test_no_name_attribute_returns_none(self):
        acl = MagicMock(spec=[])  # No attributes
        del acl.name
        del acl.resource_name
        acl = MagicMock()
        acl.name = None
        acl.resource_name = None
        acl.principal = "User:x"
        acl.host = "*"
        acl.operation = MagicMock(name="READ")
        acl.permission_type = MagicMock(name="ALLOW")
        result = _parse_binding(acl)
        assert result is None

    def test_uses_resource_name_when_name_empty(self):
        acl = _mock_acl(name="orders")
        acl.name = None
        acl.resource_name = "orders"
        result = _parse_binding(acl)
        assert result is not None
        assert result["topic"] == "orders"

    def test_different_operations_and_permissions(self):
        acl = _mock_acl(operation_name="WRITE", perm_name="DENY")
        result = _parse_binding(acl)
        assert result["operation"] == "WRITE"
        assert result["permissionType"] == "DENY"

    def test_empty_principal_and_host(self):
        acl = _mock_acl(principal="", host="")
        result = _parse_binding(acl)
        assert result["principal"] == ""
        assert result["host"] == ""


class TestFetchTopicAcls:
    """Tests for fetch_topic_acls(admin, topic_names)."""

    @patch("confluent_kafka.admin.AclBindingFilter", MagicMock())
    @patch("confluent_kafka.admin.ResourceType", MagicMock())
    @patch("confluent_kafka.admin.ResourcePatternType", MagicMock())
    @patch("confluent_kafka.admin.AclOperation", MagicMock())
    @patch("confluent_kafka.admin.AclPermissionType", MagicMock())
    def test_returns_parsed_acls_from_describe_acls(self):
        mock_admin = MagicMock()
        acl1 = _mock_acl(name="topic1", principal="User:alice")
        acl2 = _mock_acl(name="topic2", principal="User:bob")
        mock_result = MagicMock()
        mock_result.result.return_value = [acl1, acl2]
        mock_admin.describe_acls.return_value = mock_result

        result = fetch_topic_acls(mock_admin, ["topic1", "topic2"])

        assert len(result) == 2
        topics = {r["topic"] for r in result}
        assert "topic1" in topics
        assert "topic2" in topics

    def test_import_error_returns_empty_list(self):
        import builtins

        mock_admin = MagicMock()
        _orig_import = builtins.__import__

        def custom_import(name, *args, **kwargs):
            if name == "confluent_kafka.admin":
                raise ImportError("ACL classes not available")
            return _orig_import(name, *args, **kwargs)

        with patch.object(builtins, "__import__", custom_import):
            result = fetch_topic_acls(mock_admin, [])

        assert result == []

    @patch("confluent_kafka.admin.AclBindingFilter", MagicMock())
    @patch("confluent_kafka.admin.ResourceType", MagicMock())
    @patch("confluent_kafka.admin.ResourcePatternType", MagicMock())
    @patch("confluent_kafka.admin.AclOperation", MagicMock())
    @patch("confluent_kafka.admin.AclPermissionType", MagicMock())
    def test_exception_returns_empty_list(self):
        mock_admin = MagicMock()
        mock_admin.describe_acls.side_effect = Exception("Connection failed")

        result = fetch_topic_acls(mock_admin, ["topic1"])
        assert result == []

    @patch("confluent_kafka.admin.AclBindingFilter", MagicMock())
    @patch("confluent_kafka.admin.ResourceType", MagicMock())
    @patch("confluent_kafka.admin.ResourcePatternType", MagicMock())
    @patch("confluent_kafka.admin.AclOperation", MagicMock())
    @patch("confluent_kafka.admin.AclPermissionType", MagicMock())
    def test_filters_internal_topics_from_result(self):
        mock_admin = MagicMock()
        acl_internal = _mock_acl(name="__consumer_offsets")
        acl_valid = _mock_acl(name="orders")
        mock_result = MagicMock()
        mock_result.result.return_value = [acl_internal, acl_valid]
        mock_admin.describe_acls.return_value = mock_result

        result = fetch_topic_acls(mock_admin, ["__consumer_offsets", "orders"])
        assert len(result) == 1
        assert result[0]["topic"] == "orders"

    def test_per_topic_fallback_when_match_any_raises_type_error(self):
        """When match-any filter raises TypeError, falls back to per-topic."""
        mock_admin = MagicMock()

        with patch("confluent_kafka.admin.AclBindingFilter", MagicMock()):
            with patch("confluent_kafka.admin.ResourceType", MagicMock()):
                with patch("confluent_kafka.admin.ResourcePatternType", MagicMock()):
                    with patch("confluent_kafka.admin.AclOperation", MagicMock()):
                        with patch("confluent_kafka.admin.AclPermissionType", MagicMock()):
                            def describe_side_effect(acl_filter, request_timeout=None):
                                if getattr(acl_filter, "name", None) is None:
                                    raise TypeError("match-any not supported")
                                mock_result = MagicMock()
                                mock_result.result.return_value = [
                                    _mock_acl(name="orders", principal="User:alice"),
                                ]
                                return mock_result

                            mock_admin.describe_acls.side_effect = describe_side_effect

                            result = fetch_topic_acls(mock_admin, ["orders"])

        assert len(result) == 1
        assert result[0]["topic"] == "orders"
        assert result[0]["principal"] == "User:alice"

    @patch("confluent_kafka.admin.AclBindingFilter")
    @patch("confluent_kafka.admin.ResourceType", MagicMock())
    @patch("confluent_kafka.admin.ResourcePatternType", MagicMock())
    @patch("confluent_kafka.admin.AclOperation", MagicMock())
    @patch("confluent_kafka.admin.AclPermissionType", MagicMock())
    def test_topic_names_none_discovers_from_list_topics(self, mock_acl_filter_cls):
        """When topic_names is None, discovers topics from list_topics."""
        mock_admin = MagicMock()

        mock_metadata = MagicMock()
        mock_metadata.topics = {"orders": MagicMock(), "__consumer_offsets": MagicMock()}
        mock_admin.list_topics.return_value = mock_metadata

        def make_filter(*args, **kwargs):
            m = MagicMock()
            for k, v in kwargs.items():
                setattr(m, k, v)
            return m

        mock_acl_filter_cls.side_effect = make_filter

        def describe_side_effect(acl_filter, request_timeout=None):
            name = getattr(acl_filter, "name", None)
            if name is None:
                raise TypeError("match-any not supported")
            if name == "orders":
                mock_result = MagicMock()
                mock_result.result.return_value = [_mock_acl(name="orders", principal="User:bob")]
                return mock_result
            raise Exception("unexpected")

        mock_admin.describe_acls.side_effect = describe_side_effect

        result = fetch_topic_acls(mock_admin, None)

        assert len(result) == 1
        assert result[0]["topic"] == "orders"

    @patch("confluent_kafka.admin.AclBindingFilter")
    @patch("confluent_kafka.admin.ResourceType", MagicMock())
    @patch("confluent_kafka.admin.ResourcePatternType", MagicMock())
    @patch("confluent_kafka.admin.AclOperation", MagicMock())
    @patch("confluent_kafka.admin.AclPermissionType", MagicMock())
    def test_deduplication_of_acl_bindings_in_per_topic_path(self, mock_acl_filter_cls):
        """Duplicate ACL bindings are deduplicated in per-topic path."""
        mock_admin = MagicMock()

        def make_filter(*args, **kwargs):
            m = MagicMock()
            for k, v in kwargs.items():
                setattr(m, k, v)
            return m

        mock_acl_filter_cls.side_effect = make_filter

        acl1 = _mock_acl(name="orders", principal="User:alice", host="*", operation_name="READ", perm_name="ALLOW")
        acl2 = _mock_acl(name="orders", principal="User:alice", host="*", operation_name="READ", perm_name="ALLOW")

        mock_result = MagicMock()
        mock_result.result.return_value = [acl1, acl2]

        def describe_side_effect(acl_filter, request_timeout=None):
            if getattr(acl_filter, "name", None) is None:
                raise TypeError("match-any not supported")
            return mock_result

        mock_admin.describe_acls.side_effect = describe_side_effect

        result = fetch_topic_acls(mock_admin, ["orders"])

        assert len(result) == 1
        assert result[0]["topic"] == "orders"
        assert result[0]["principal"] == "User:alice"
