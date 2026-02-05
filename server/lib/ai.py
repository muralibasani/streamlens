import os
import json
import re

# Provider: "openai" | "gemini" | "anthropic" | "ollama" from env, or auto-detect from which API key is set
def _get_provider() -> str:
    explicit = (os.environ.get("AI_PROVIDER") or "").strip().lower()
    if explicit in ("openai", "gemini", "anthropic", "claude", "ollama"):
        return "anthropic" if explicit == "claude" else explicit
    openai_key = (os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY") or "").strip()
    gemini_key = (os.environ.get("AI_INTEGRATIONS_GEMINI_API_KEY") or "").strip()
    anthropic_key = (os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY") or "").strip()
    if openai_key and openai_key != "dummy":
        return "openai"
    if anthropic_key:
        return "anthropic"
    if gemini_key and gemini_key != "dummy":
        return "gemini"
    return "openai"  # default when no key set

# Lazy OpenAI client (only when provider is openai)
_openai_client = None

def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        from openai import OpenAI
        _openai_client = OpenAI(
            api_key=os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY", "dummy"),
            base_url=os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL"),
        )
    return _openai_client

# Lazy Anthropic client (only when provider is anthropic)
_anthropic_client = None

def _get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        try:
            from anthropic import Anthropic
        except ImportError:
            raise RuntimeError(
                "Anthropic provider selected but anthropic is not installed. "
                "Install with: uv sync --extra anthropic  or  pip install anthropic"
            )
        api_key = (os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY") or "").strip()
        if not api_key:
            raise ValueError("AI_INTEGRATIONS_ANTHROPIC_API_KEY is not set")
        _anthropic_client = Anthropic(api_key=api_key)
    return _anthropic_client

# Lazy Ollama client (OpenAI-compatible API; only when provider is ollama)
_ollama_client = None

def _get_ollama_client():
    global _ollama_client
    if _ollama_client is None:
        from openai import OpenAI
        base = (os.environ.get("OLLAMA_BASE_URL") or "http://localhost:11434").strip().rstrip("/")
        _ollama_client = OpenAI(base_url=f"{base}/v1", api_key="ollama")
    return _ollama_client

# Lazy Gemini model (only when provider is gemini)
_gemini_model = None

def _get_gemini_model():
    global _gemini_model
    if _gemini_model is None:
        try:
            import google.generativeai as genai
        except ImportError:
            raise RuntimeError(
                "Gemini provider selected but google-generativeai is not installed. "
                "Install with: uv sync --extra gemini  or  pip install google-generativeai"
            )
        api_key = (os.environ.get("AI_INTEGRATIONS_GEMINI_API_KEY") or "").strip()
        if not api_key:
            raise ValueError("AI_INTEGRATIONS_GEMINI_API_KEY is not set")
        genai.configure(api_key=api_key)
        model_name = os.environ.get("AI_INTEGRATIONS_GEMINI_MODEL", "gemini-2.0-flash")
        _gemini_model = genai.GenerativeModel(model_name)
    return _gemini_model


SYSTEM_PROMPT = "You are a helpful Kafka expert."
PROMPT_TEMPLATE = """
You are StreamPilot, a Kafka topology reasoning assistant.
You are given a graph with nodes and edges representing Kafka topics, producers, consumers, streams applications, connectors, and schemas.

Current Topology Graph JSON:
{topology_json}

User question:
{question}

Using the graph:
1. Analyze the nodes and edges to find relevant entities that answer the question.
2. Provide a clear, concise answer in plain English.
3. Return the exact node IDs (from the graph) of all relevant entities to highlight and zoom to in the UI.

Important:
- For questions about "producers writing to X topic", include producer node IDs and the topic node ID
- For questions about "consumers of X topic", include consumer node IDs and the topic node ID
- For questions about "topics produced by X", include the producer/app node ID and all relevant topic node IDs
- For questions about schemas, include schema node IDs (format: "schema:subject-name") and their linked topic nodes
- For questions about connectors, include connector node IDs (format: "connect:connector-name") and their linked topic nodes
- For questions about ACLs on a topic, include ACL node IDs (format: "acl:topic:topicname") and the topic node
- Always include the full node ID as it appears in the graph (e.g., "topic:testtopic", "group:mygroup", "jmx:active-producer:testtopic", "schema:orders-value", "connect:file-source", "acl:topic:transactions-topic")

Respond with ONLY a single JSON object, no other text or markdown. Use this exact structure:
{{"answer": "Plain English explanation...", "highlightNodes": ["topic:orders", "group:checkout-consumer"]}}
"""


def _parse_json_from_text(text: str) -> dict:
    """Strip markdown code fences and parse JSON."""
    text = (text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```\s*$", "", text)
    return json.loads(text or "{}")


def _query_openai(question: str, topology: dict) -> dict:
    client = _get_openai_client()
    prompt = PROMPT_TEMPLATE.format(topology_json=json.dumps(topology), question=question)
    response = client.chat.completions.create(
        model=os.environ.get("AI_INTEGRATIONS_OPENAI_MODEL", "gpt-4o-mini"),
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
    )
    content = response.choices[0].message.content or "{}"
    return json.loads(content)


def _query_gemini(question: str, topology: dict) -> dict:
    model = _get_gemini_model()
    prompt = PROMPT_TEMPLATE.format(topology_json=json.dumps(topology), question=question)
    response = model.generate_content(
        prompt,
        generation_config={"temperature": 0.2},
    )
    text = (response.text or "").strip()
    return _parse_json_from_text(text)


def _query_anthropic(question: str, topology: dict) -> dict:
    client = _get_anthropic_client()
    prompt = PROMPT_TEMPLATE.format(topology_json=json.dumps(topology), question=question)
    model = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_MODEL", "claude-3-5-haiku-20241022")
    message = client.messages.create(
        model=model,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    text = ""
    for block in message.content:
        if hasattr(block, "text") and block.text:
            text += block.text
    return _parse_json_from_text(text)


def _query_ollama(question: str, topology: dict) -> dict:
    client = _get_ollama_client()
    prompt = PROMPT_TEMPLATE.format(topology_json=json.dumps(topology), question=question)
    model = os.environ.get("OLLAMA_MODEL", "llama3.2")
    # Ollama's OpenAI-compatible API; many models don't support response_format so we parse text
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    )
    content = response.choices[0].message.content or "{}"
    return _parse_json_from_text(content)


def query_topology(question: str, topology: dict) -> dict:
    provider = _get_provider()
    try:
        if provider == "gemini":
            result = _query_gemini(question, topology)
        elif provider == "anthropic":
            result = _query_anthropic(question, topology)
        elif provider == "ollama":
            result = _query_ollama(question, topology)
        else:
            result = _query_openai(question, topology)
        if "highlightNodes" not in result:
            result["highlightNodes"] = []
        if "answer" not in result:
            result["answer"] = "No answer generated."
        return result
    except Exception as e:
        print(f"AI Query failed ({provider}): {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

        if provider == "openai":
            api_key = (os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY") or "").strip()
            if not api_key or api_key == "dummy":
                error_msg = (
                    "AI assistant not configured. Set AI_INTEGRATIONS_OPENAI_API_KEY, or use Gemini (AI_PROVIDER=gemini), "
                    "Anthropic (AI_PROVIDER=anthropic), or Ollama (AI_PROVIDER=ollama). See docs/AI_SETUP.md."
                )
            else:
                error_msg = f"I couldn't process your request: {str(e)}"
        elif provider == "ollama":
            error_msg = (
                "Ollama not reachable. Set AI_PROVIDER=ollama, ensure Ollama is running (e.g. ollama serve), "
                "and optionally OLLAMA_BASE_URL (default http://localhost:11434), OLLAMA_MODEL (default llama3.2). See docs/AI_SETUP.md."
            )
            if "connection" not in str(e).lower() and "refused" not in str(e).lower():
                error_msg = f"I couldn't process your request: {str(e)}"
        elif provider == "anthropic":
            api_key = (os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY") or "").strip()
            if not api_key:
                error_msg = (
                    "Anthropic not configured. Set AI_INTEGRATIONS_ANTHROPIC_API_KEY (and optionally AI_PROVIDER=anthropic or claude). "
                    "Install with: uv sync --extra anthropic. See docs/AI_SETUP.md."
                )
            else:
                error_msg = f"I couldn't process your request: {str(e)}"
        else:
            api_key = (os.environ.get("AI_INTEGRATIONS_GEMINI_API_KEY") or "").strip()
            if not api_key:
                error_msg = (
                    "Gemini not configured. Set AI_INTEGRATIONS_GEMINI_API_KEY (and optionally AI_PROVIDER=gemini). "
                    "See docs/AI_SETUP.md."
                )
            else:
                error_msg = f"I couldn't process your request: {str(e)}"

        return {
            "answer": error_msg,
            "highlightNodes": [],
        }
