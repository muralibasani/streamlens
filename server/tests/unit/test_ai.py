"""Unit tests for server/src/ai.py."""
import builtins
import importlib.util
from unittest.mock import patch, MagicMock

import pytest

import src.ai as ai_mod

HAS_ANTHROPIC = importlib.util.find_spec("anthropic") is not None


def _reset_ai_clients():
    """Reset lazy singleton clients between tests."""
    ai_mod._openai_client = None
    ai_mod._anthropic_client = None
    ai_mod._ollama_client = None
    ai_mod._gemini_model = None


@pytest.fixture(autouse=True)
def _reset_clients():
    """Reset AI clients before and after each test."""
    _reset_ai_clients()
    yield
    _reset_ai_clients()


class TestGetProvider:
    """Tests for _get_provider()."""

    def test_explicit_openai(self, monkeypatch):
        monkeypatch.delenv("AI_PROVIDER", raising=False)
        monkeypatch.setenv("AI_PROVIDER", "openai")
        monkeypatch.delenv("AI_INTEGRATIONS_OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_GEMINI_API_KEY", raising=False)
        assert ai_mod._get_provider() == "openai"

    def test_explicit_gemini(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "gemini")
        monkeypatch.delenv("AI_INTEGRATIONS_OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_GEMINI_API_KEY", raising=False)
        assert ai_mod._get_provider() == "gemini"

    def test_explicit_anthropic(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "anthropic")
        monkeypatch.delenv("AI_INTEGRATIONS_OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_GEMINI_API_KEY", raising=False)
        assert ai_mod._get_provider() == "anthropic"

    def test_claude_maps_to_anthropic(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "claude")
        monkeypatch.delenv("AI_INTEGRATIONS_OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_GEMINI_API_KEY", raising=False)
        assert ai_mod._get_provider() == "anthropic"

    def test_explicit_ollama(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "ollama")
        monkeypatch.delenv("AI_INTEGRATIONS_OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_GEMINI_API_KEY", raising=False)
        assert ai_mod._get_provider() == "ollama"

    def test_auto_detect_openai_key(self, monkeypatch):
        monkeypatch.delenv("AI_PROVIDER", raising=False)
        monkeypatch.setenv("AI_INTEGRATIONS_OPENAI_API_KEY", "sk-fake")
        monkeypatch.delenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_GEMINI_API_KEY", raising=False)
        assert ai_mod._get_provider() == "openai"

    def test_auto_detect_anthropic_key(self, monkeypatch):
        monkeypatch.delenv("AI_PROVIDER", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_OPENAI_API_KEY", raising=False)
        monkeypatch.setenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", "sk-ant-fake")
        monkeypatch.delenv("AI_INTEGRATIONS_GEMINI_API_KEY", raising=False)
        assert ai_mod._get_provider() == "anthropic"

    def test_auto_detect_gemini_key(self, monkeypatch):
        monkeypatch.delenv("AI_PROVIDER", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", raising=False)
        monkeypatch.setenv("AI_INTEGRATIONS_GEMINI_API_KEY", "fake-gemini-key")
        assert ai_mod._get_provider() == "gemini"

    def test_openai_dummy_key_not_counted(self, monkeypatch):
        """'dummy' key should not count as configured; falls through to default."""
        monkeypatch.delenv("AI_PROVIDER", raising=False)
        monkeypatch.setenv("AI_INTEGRATIONS_OPENAI_API_KEY", "dummy")
        monkeypatch.delenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("AI_INTEGRATIONS_GEMINI_API_KEY", raising=False)
        # With only dummy openai key, no real key; defaults to openai
        assert ai_mod._get_provider() == "openai"


class TestGetAiStatus:
    """Tests for get_ai_status()."""

    def test_openai_with_key(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "openai")
        monkeypatch.setenv("AI_INTEGRATIONS_OPENAI_API_KEY", "sk-real")
        assert ai_mod.get_ai_status()["provider"] == "openai"
        assert ai_mod.get_ai_status()["configured"] is True

    def test_openai_without_key(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "openai")
        monkeypatch.delenv("AI_INTEGRATIONS_OPENAI_API_KEY", raising=False)
        status = ai_mod.get_ai_status()
        assert status["provider"] is None
        assert status["configured"] is False

    def test_gemini_with_key(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "gemini")
        monkeypatch.setenv("AI_INTEGRATIONS_GEMINI_API_KEY", "fake-key")
        assert ai_mod.get_ai_status()["provider"] == "gemini"
        assert ai_mod.get_ai_status()["configured"] is True

    def test_anthropic_with_key(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "anthropic")
        monkeypatch.setenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", "sk-ant-fake")
        assert ai_mod.get_ai_status()["provider"] == "anthropic"
        assert ai_mod.get_ai_status()["configured"] is True

    def test_ollama_always_configured(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "ollama")
        status = ai_mod.get_ai_status()
        assert status["provider"] == "ollama"
        assert status["configured"] is True

    def test_gemini_without_key(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "gemini")
        monkeypatch.delenv("AI_INTEGRATIONS_GEMINI_API_KEY", raising=False)
        status = ai_mod.get_ai_status()
        assert status["provider"] is None
        assert status["configured"] is False


class TestIsMutatingRequest:
    """Tests for _is_mutating_request()."""

    def test_delete_topic(self):
        assert ai_mod._is_mutating_request("delete topic orders") is True

    def test_create_topic(self):
        assert ai_mod._is_mutating_request("create topic my-topic") is True

    def test_produce_a_message(self):
        assert ai_mod._is_mutating_request("produce a message to orders") is True

    def test_show_me_topics_not_mutating(self):
        assert ai_mod._is_mutating_request("show me topics") is False

    def test_empty_string(self):
        assert ai_mod._is_mutating_request("") is False

    def test_what_topics_exist_not_mutating(self):
        assert ai_mod._is_mutating_request("what topics exist") is False


class TestParseJsonFromText:
    """Tests for _parse_json_from_text()."""

    def test_valid_json(self):
        text = '{"answer": "yes", "highlightNodes": []}'
        result = ai_mod._parse_json_from_text(text)
        assert result == {"answer": "yes", "highlightNodes": []}

    def test_with_markdown_code_fences(self):
        text = '```json\n{"answer": "hi"}\n```'
        result = ai_mod._parse_json_from_text(text)
        assert result == {"answer": "hi"}

    def test_empty_string(self):
        result = ai_mod._parse_json_from_text("")
        assert result == {}

    def test_invalid_json_raises(self):
        with pytest.raises(Exception):
            ai_mod._parse_json_from_text("not valid json {")


class TestQueryTopology:
    """Tests for query_topology()."""

    def test_mutating_request_returns_read_only_decline(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "openai")
        monkeypatch.setenv("AI_INTEGRATIONS_OPENAI_API_KEY", "sk-fake")
        topology = {"nodes": [], "edges": []}

        result = ai_mod.query_topology("delete topic orders", topology)

        assert result == ai_mod.READ_ONLY_DECLINE

    def test_non_mutating_calls_provider(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "openai")
        monkeypatch.setenv("AI_INTEGRATIONS_OPENAI_API_KEY", "sk-fake")

        fake_response = {"answer": "There are 3 topics.", "highlightNodes": ["topic:orders"]}

        with patch("src.ai._query_openai", return_value=fake_response) as mock_query:
            result = ai_mod.query_topology("show me topics", {"nodes": [], "edges": []})

        mock_query.assert_called_once()
        assert result["answer"] == "There are 3 topics."
        assert "topic:orders" in result["highlightNodes"]

    def test_non_mutating_calls_anthropic_when_provider_anthropic(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "anthropic")
        monkeypatch.setenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", "sk-ant-fake")

        fake_response = {"answer": "Done.", "highlightNodes": []}

        with patch("src.ai._query_anthropic", return_value=fake_response) as mock_query:
            result = ai_mod.query_topology("what topics exist", {"nodes": [], "edges": []})

        mock_query.assert_called_once()
        assert result["answer"] == "Done."

    def test_non_mutating_calls_gemini_when_provider_gemini(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "gemini")
        monkeypatch.setenv("AI_INTEGRATIONS_GEMINI_API_KEY", "fake-key")

        fake_response = {"answer": "Topics: a, b.", "highlightNodes": []}

        with patch("src.ai._query_gemini", return_value=fake_response) as mock_query:
            result = ai_mod.query_topology("list topics", {"nodes": [], "edges": []})

        mock_query.assert_called_once()
        assert result["answer"] == "Topics: a, b."

    # --- Error handling tests ---

    def test_query_topology_openai_no_key_error(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "openai")
        monkeypatch.delenv("AI_INTEGRATIONS_OPENAI_API_KEY", raising=False)

        with patch("src.ai._query_openai", side_effect=Exception("Connection refused")):
            result = ai_mod.query_topology("what topics exist?", {"nodes": [], "edges": []})

        assert "not configured" in result["answer"].lower() or "AI_INTEGRATIONS_OPENAI_API_KEY" in result["answer"]
        assert result["highlightNodes"] == []

    def test_query_topology_openai_dummy_key_error(self, monkeypatch):
        """When API key is 'dummy', treated as not configured."""
        monkeypatch.setenv("AI_PROVIDER", "openai")
        monkeypatch.setenv("AI_INTEGRATIONS_OPENAI_API_KEY", "dummy")

        with patch("src.ai._query_openai", side_effect=Exception("Invalid API key")):
            result = ai_mod.query_topology("what topics exist?", {"nodes": [], "edges": []})

        assert "not configured" in result["answer"].lower()

    def test_query_topology_gemini_with_key_generic_error(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "gemini")
        monkeypatch.setenv("AI_INTEGRATIONS_GEMINI_API_KEY", "fake-key")

        with patch("src.ai._query_gemini", side_effect=Exception("Rate limit exceeded")):
            result = ai_mod.query_topology("what topics exist?", {"nodes": [], "edges": []})

        assert "couldn't process" in result["answer"].lower() or "Rate limit" in result["answer"]

    def test_query_topology_openai_with_key_generic_error(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "openai")
        monkeypatch.setenv("AI_INTEGRATIONS_OPENAI_API_KEY", "sk-real-key")

        with patch("src.ai._query_openai", side_effect=Exception("Rate limit exceeded")):
            result = ai_mod.query_topology("what topics exist?", {"nodes": [], "edges": []})

        assert "Rate limit exceeded" in result["answer"]
        assert result["highlightNodes"] == []

    def test_query_topology_gemini_no_key_error(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "gemini")
        monkeypatch.delenv("AI_INTEGRATIONS_GEMINI_API_KEY", raising=False)
        monkeypatch.setenv("AI_INTEGRATIONS_GEMINI_API_KEY", "")

        with patch("src.ai._query_gemini", side_effect=Exception("API key not configured")):
            result = ai_mod.query_topology("what topics exist?", {"nodes": [], "edges": []})

        assert "not configured" in result["answer"].lower() or "AI_INTEGRATIONS_GEMINI_API_KEY" in result["answer"]

    def test_query_topology_anthropic_no_key_error(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "anthropic")
        monkeypatch.delenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", raising=False)
        monkeypatch.setenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", "")

        with patch("src.ai._query_anthropic", side_effect=Exception("API key required")):
            result = ai_mod.query_topology("what topics exist?", {"nodes": [], "edges": []})

        assert "not configured" in result["answer"].lower() or "AI_INTEGRATIONS_ANTHROPIC_API_KEY" in result["answer"]

    def test_query_topology_ollama_connection_error(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "ollama")

        with patch("src.ai._query_ollama", side_effect=Exception("Connection refused")):
            result = ai_mod.query_topology("what topics exist?", {"nodes": [], "edges": []})

        assert "ollama" in result["answer"].lower() or "not reachable" in result["answer"].lower()

    def test_query_topology_ollama_non_connection_error(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "ollama")

        with patch("src.ai._query_ollama", side_effect=Exception("Invalid JSON response")):
            result = ai_mod.query_topology("what topics exist?", {"nodes": [], "edges": []})

        assert "couldn't process" in result["answer"].lower() or "Invalid JSON" in result["answer"]

    def test_query_topology_successful_response_missing_highlight_nodes(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "openai")
        monkeypatch.setenv("AI_INTEGRATIONS_OPENAI_API_KEY", "sk-fake")

        with patch("src.ai._query_openai", return_value={"answer": "There are 3 topics."}):
            result = ai_mod.query_topology("show me topics", {"nodes": [], "edges": []})

        assert result["highlightNodes"] == []
        assert result["answer"] == "There are 3 topics."

    def test_query_topology_successful_response_missing_answer(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "openai")
        monkeypatch.setenv("AI_INTEGRATIONS_OPENAI_API_KEY", "sk-fake")

        with patch("src.ai._query_openai", return_value={"highlightNodes": ["topic:orders"]}):
            result = ai_mod.query_topology("show me topics", {"nodes": [], "edges": []})

        assert result["answer"] == "No answer generated."
        assert result["highlightNodes"] == ["topic:orders"]

    def test_non_mutating_calls_ollama_when_provider_ollama(self, monkeypatch):
        monkeypatch.setenv("AI_PROVIDER", "ollama")

        fake_response = {"answer": "Ollama says hi.", "highlightNodes": []}

        with patch("src.ai._query_ollama", return_value=fake_response) as mock_query:
            result = ai_mod.query_topology("what topics exist", {"nodes": [], "edges": []})

        mock_query.assert_called_once()
        assert result["answer"] == "Ollama says hi."


class TestQueryOpenai:
    """Tests for _query_openai() with mocked client."""

    @patch("src.ai._get_openai_client")
    def test_query_openai_returns_parsed_response(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = '{"answer": "test", "highlightNodes": ["topic:t1"]}'
        mock_client.chat.completions.create.return_value = mock_response

        from src.ai import _query_openai

        result = _query_openai("test question", {"nodes": [], "edges": []})
        assert result["answer"] == "test"
        assert result["highlightNodes"] == ["topic:t1"]


class TestQueryGemini:
    """Tests for _query_gemini() with mocked model."""

    @patch("src.ai._get_gemini_model")
    def test_query_gemini_returns_parsed_response(self, mock_get_model):
        mock_model = MagicMock()
        mock_get_model.return_value = mock_model
        mock_response = MagicMock()
        mock_response.text = '{"answer": "gemini answer", "highlightNodes": []}'
        mock_model.generate_content.return_value = mock_response

        from src.ai import _query_gemini

        result = _query_gemini("test question", {"nodes": [], "edges": []})
        assert result["answer"] == "gemini answer"


class TestQueryAnthropic:
    """Tests for _query_anthropic() with mocked client."""

    @patch("src.ai._get_anthropic_client")
    def test_query_anthropic_returns_parsed_response(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_block = MagicMock()
        mock_block.text = '{"answer": "anthropic answer", "highlightNodes": []}'
        mock_message = MagicMock()
        mock_message.content = [mock_block]
        mock_client.messages.create.return_value = mock_message

        from src.ai import _query_anthropic

        result = _query_anthropic("test question", {"nodes": [], "edges": []})
        assert result["answer"] == "anthropic answer"

    @patch("src.ai._get_anthropic_client")
    def test_query_anthropic_ignores_non_text_blocks(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        text_block = MagicMock()
        text_block.text = '{"answer": "yes"}'
        # First block: use object() so hasattr(block, "text") is False
        non_text_block = object()
        mock_message = MagicMock()
        mock_message.content = [non_text_block, text_block]
        mock_client.messages.create.return_value = mock_message

        from src.ai import _query_anthropic

        result = _query_anthropic("q", {"nodes": []})
        assert result["answer"] == "yes"


class TestQueryOllama:
    """Tests for _query_ollama() with mocked client."""

    @patch("src.ai._get_ollama_client")
    def test_query_ollama_returns_parsed_response(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = '{"answer": "ollama answer", "highlightNodes": []}'
        mock_client.chat.completions.create.return_value = mock_response

        from src.ai import _query_ollama

        result = _query_ollama("test question", {"nodes": [], "edges": []})
        assert result["answer"] == "ollama answer"


class TestGetOpenaiClient:
    """Tests for _get_openai_client()."""

    @patch("openai.OpenAI")
    def test_get_openai_client_creates_client_when_none(self, mock_openai_cls, monkeypatch):
        monkeypatch.setenv("AI_INTEGRATIONS_OPENAI_API_KEY", "test-key")
        _reset_ai_clients()
        ai_mod._openai_client = None

        from src.ai import _get_openai_client

        client = _get_openai_client()
        mock_openai_cls.assert_called_once()
        assert client is mock_openai_cls.return_value

    @patch("openai.OpenAI")
    def test_get_openai_client_returns_cached_on_second_call(self, mock_openai_cls):
        ai_mod._openai_client = MagicMock()
        cached = ai_mod._openai_client

        from src.ai import _get_openai_client

        client = _get_openai_client()
        assert client is cached
        mock_openai_cls.assert_not_called()


class TestGetAnthropicClient:
    """Tests for _get_anthropic_client()."""

    @pytest.mark.skipif(not HAS_ANTHROPIC, reason="anthropic not installed")
    @patch("anthropic.Anthropic")
    def test_get_anthropic_client_creates_client_when_configured(self, mock_anthropic_cls, monkeypatch):
        monkeypatch.setenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", "sk-ant-fake")
        ai_mod._anthropic_client = None

        from src.ai import _get_anthropic_client

        _get_anthropic_client()
        mock_anthropic_cls.assert_called_once()

    def test_get_anthropic_client_raises_on_import_error(self, monkeypatch):
        monkeypatch.setenv("AI_INTEGRATIONS_ANTHROPIC_API_KEY", "sk-ant-fake")
        ai_mod._anthropic_client = None

        class FakeAnthropicModule:
            def __getattr__(self, name):
                raise ImportError("no module named anthropic")

        import sys

        with patch.dict(sys.modules, {"anthropic": FakeAnthropicModule()}):
            with pytest.raises(RuntimeError, match="anthropic is not installed"):
                from src.ai import _get_anthropic_client

                _get_anthropic_client()


class TestGetGeminiModel:
    """Tests for _get_gemini_model()."""

    def test_get_gemini_model_raises_on_import_error(self, monkeypatch):
        monkeypatch.setenv("AI_INTEGRATIONS_GEMINI_API_KEY", "fake-key")
        ai_mod._gemini_model = None

        orig_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "google.generativeai":
                raise ImportError("google-generativeai not installed")
            return orig_import(name, *args, **kwargs)

        with patch("builtins.__import__", fake_import):
            with pytest.raises(RuntimeError, match="google-generativeai is not installed"):
                from src.ai import _get_gemini_model

                _get_gemini_model()
