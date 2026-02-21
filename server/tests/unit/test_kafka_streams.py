"""Unit tests for server/src/kafka/streams.py."""
from unittest.mock import MagicMock, mock_open, patch

import pytest

from src.kafka.streams import load_streams_config


def _mock_path_exists(exists: bool):
    """Return a MagicMock for Path that has exists() return the given value."""
    mock_config = MagicMock()
    mock_config.exists.return_value = exists
    mock_path = MagicMock()
    mock_path.parent.parent.parent.__truediv__.return_value = mock_config
    return mock_path


class TestLoadStreamsConfig:
    """Tests for load_streams_config()."""

    def test_no_streams_yaml_exists_returns_empty(self):
        with patch("src.kafka.streams.Path", return_value=_mock_path_exists(False)):
            result = load_streams_config()
        assert result == []

    def test_valid_streams_one_app(self):
        yaml_content = """streams:
  - name: my-app
    consumerGroup: my-app-group
    inputTopics: [input]
    outputTopics: [output]
"""
        with patch("src.kafka.streams.Path", return_value=_mock_path_exists(True)):
            with patch("builtins.open", mock_open(read_data=yaml_content)):
                result = load_streams_config()

        assert len(result) == 1
        assert result[0]["name"] == "my-app"
        assert result[0]["consumerGroup"] == "my-app-group"
        assert result[0]["id"] == "streams:my-app"
        assert result[0]["consumesFrom"] == ["input"]
        assert result[0]["producesTo"] == ["output"]

    def test_valid_streams_multiple_apps(self):
        yaml_content = """streams:
  - name: app-one
    consumerGroup: cg-one
    inputTopics: [in1]
    outputTopics: [out1]
  - name: app-two
    consumerGroup: cg-two
    inputTopics: [in2]
    outputTopics: [out2]
"""
        with patch("src.kafka.streams.Path", return_value=_mock_path_exists(True)):
            with patch("builtins.open", mock_open(read_data=yaml_content)):
                result = load_streams_config()

        assert len(result) == 2
        assert result[0]["name"] == "app-one"
        assert result[1]["name"] == "app-two"

    def test_no_streams_key_returns_empty(self):
        """streams.yaml exists but has no 'streams' key."""
        yaml_content = """other_key: []
"""
        with patch("src.kafka.streams.Path", return_value=_mock_path_exists(True)):
            with patch("builtins.open", mock_open(read_data=yaml_content)):
                result = load_streams_config()

        assert result == []

    def test_stream_missing_name_skipped(self):
        yaml_content = """streams:
  - consumerGroup: cg-only
    inputTopics: [in]
    outputTopics: [out]
"""
        with patch("src.kafka.streams.Path", return_value=_mock_path_exists(True)):
            with patch("builtins.open", mock_open(read_data=yaml_content)):
                result = load_streams_config()

        assert len(result) == 0

    def test_stream_missing_consumer_group_skipped(self):
        yaml_content = """streams:
  - name: no-cg
    inputTopics: [in]
    outputTopics: [out]
"""
        with patch("src.kafka.streams.Path", return_value=_mock_path_exists(True)):
            with patch("builtins.open", mock_open(read_data=yaml_content)):
                result = load_streams_config()

        assert len(result) == 0

    def test_malformed_yaml_returns_empty(self):
        """Exception during load returns [] (caught)."""
        with patch("src.kafka.streams.Path", return_value=_mock_path_exists(True)):
            with patch("builtins.open", mock_open(read_data="invalid: yaml: [:")):
                with patch("src.kafka.streams.yaml.safe_load") as mock_load:
                    mock_load.side_effect = Exception("parse error")
                    result = load_streams_config()

        assert result == []
