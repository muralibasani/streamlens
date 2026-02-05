# AI Chat Assistant Setup

The topology view includes an AI-powered chat assistant that can answer questions about your Kafka cluster.

## Supported providers

- **OpenAI** (default) – OpenAI API, Azure OpenAI, or any OpenAI-compatible endpoint (e.g. LM Studio).
- **Google Gemini** – Google’s Gemini API.
- **Anthropic (Claude)** – Anthropic’s Claude API.
- **Ollama** – Local LLM (no API key; runs on your machine).

## Configuration

### 1. Set environment variables

Create a `.env` file in the `server/` directory (or use `.env.dev`):

```bash
cd server
cp .env.example .env
```

Then configure **one** of the following.

---

### Option A: OpenAI (default)

```bash
AI_INTEGRATIONS_OPENAI_API_KEY=sk-your-actual-api-key-here
```

Get an API key from: https://platform.openai.com/api-keys

Optional:

- `AI_INTEGRATIONS_OPENAI_BASE_URL` – override endpoint (e.g. Azure or local LLM).
- `AI_INTEGRATIONS_OPENAI_MODEL` – model name (default: `gpt-4o-mini`).

---

### Option B: Google Gemini

1. Install the Gemini extra (if not already installed):

   ```bash
   cd server
   uv sync --extra gemini
   # or: pip install google-generativeai
   ```

2. Get an API key from: https://aistudio.google.com/apikey

3. Set in `.env`:

   ```bash
   AI_PROVIDER=gemini
   AI_INTEGRATIONS_GEMINI_API_KEY=your-gemini-api-key
   ```

   Optional:

   - `AI_INTEGRATIONS_GEMINI_MODEL` – model name (default: `gemini-2.0-flash`).

If only `AI_INTEGRATIONS_GEMINI_API_KEY` is set (and no OpenAI key), the server will use Gemini automatically; you can still set `AI_PROVIDER=gemini` to force Gemini when multiple keys are present.

---

### Option C: Anthropic (Claude)

1. Install the Anthropic extra (if not already installed):

   ```bash
   cd server
   uv sync --extra anthropic
   # or: pip install anthropic
   ```

2. Get an API key from: https://console.anthropic.com/ (API keys)

3. Set in `.env`:

   ```bash
   AI_PROVIDER=anthropic
   AI_INTEGRATIONS_ANTHROPIC_API_KEY=your-anthropic-api-key
   ```

   You can also use `AI_PROVIDER=claude` (treated the same as `anthropic`).

   Optional:

   - `AI_INTEGRATIONS_ANTHROPIC_MODEL` – model name (default: `claude-3-5-haiku-20241022`).

If only `AI_INTEGRATIONS_ANTHROPIC_API_KEY` is set (and no OpenAI key), the server will use Anthropic automatically.

---

### Option D: Ollama (local LLM)

No API key required. Uses the existing `openai` package (Ollama exposes an OpenAI-compatible API).

1. Install and run Ollama: https://ollama.com

   ```bash
   ollama serve    # usually runs by default
   ollama pull llama3.2
   ```

2. Set in `.env`:

   ```bash
   AI_PROVIDER=ollama
   ```

   Optional:

   - `OLLAMA_BASE_URL` – base URL (default: `http://localhost:11434`).
   - `OLLAMA_MODEL` – model name (default: `llama3.2`). Use any model you’ve pulled (e.g. `llama3.2`, `mistral`, `codellama`).

Example:

```bash
AI_PROVIDER=ollama
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=llama3.2
```

---

### 2. Load environment variables

The server loads `.env` via `python-dotenv` (see `main.py`). Ensure your `.env` or `.env.dev` is in the `server/` directory.

### 3. Restart the server

```bash
cd server
uv run python main.py
```

## Alternative: Other OpenAI-compatible endpoints

You can use any OpenAI-compatible API (e.g. LM Studio, Azure OpenAI) by using the **OpenAI** provider and setting the base URL:

### Local LLM (LM Studio, etc.)

```bash
AI_INTEGRATIONS_OPENAI_API_KEY=dummy
AI_INTEGRATIONS_OPENAI_BASE_URL=http://localhost:1234/v1
```

For **Ollama** specifically, use **Option D** above (`AI_PROVIDER=ollama`) so you don’t need a dummy key.

### Azure OpenAI

```bash
AI_INTEGRATIONS_OPENAI_API_KEY=your-azure-api-key
AI_INTEGRATIONS_OPENAI_BASE_URL=https://your-resource.openai.azure.com/
```

## Troubleshooting

### Error: "AI assistant not configured"

**Cause**: No API key set for the selected provider.

**Solution**:

- For **OpenAI**: set `AI_INTEGRATIONS_OPENAI_API_KEY`.
- For **Gemini**: set `AI_INTEGRATIONS_GEMINI_API_KEY` and optionally `AI_PROVIDER=gemini`. Install: `uv sync --extra gemini`.
- For **Anthropic**: set `AI_INTEGRATIONS_ANTHROPIC_API_KEY` and optionally `AI_PROVIDER=anthropic` or `AI_PROVIDER=claude`. Install: `uv sync --extra anthropic`.
- For **Ollama**: set `AI_PROVIDER=ollama`, ensure Ollama is running (`ollama serve`), and optionally `OLLAMA_BASE_URL`, `OLLAMA_MODEL`.

### Error: "Ollama not reachable"

**Cause**: Ollama is not running or the URL is wrong.

**Solution**: Start Ollama (e.g. `ollama serve`), pull a model (`ollama pull llama3.2`), and check `OLLAMA_BASE_URL` (default `http://localhost:11434`).

### Error: "Gemini provider selected but google-generativeai is not installed"

**Cause**: Gemini is selected but the package is missing.

**Solution**:

```bash
cd server
uv sync --extra gemini
# or: pip install google-generativeai
```

### Error: "Anthropic provider selected but anthropic is not installed"

**Cause**: Anthropic is selected but the package is missing.

**Solution**:

```bash
cd server
uv sync --extra anthropic
# or: pip install anthropic
```

### Error: "I couldn't process your request at the moment"

**Cause**: Invalid key, network issue, or rate limit.

**Solution**:

1. Check `.env` (or `.env.dev`) in `server/`.
2. Verify the API key for the provider in use.
3. Check server logs for the real error (look for "AI Query failed").

## Testing

Once configured, try asking in the AI chat:

- "Which producers write to testtopic?"
- "Show me all consumers for orders topic"
- "What topics does my-app produce to?"

The assistant will analyze the topology graph and highlight relevant nodes.

## Cost considerations

- **OpenAI** (default model `gpt-4o-mini`): low cost per query; see OpenAI pricing.
- **Gemini** (default `gemini-2.0-flash`): see [Google AI pricing](https://ai.google.dev/pricing).
- **Anthropic** (default `claude-3-5-haiku`): see [Anthropic pricing](https://www.anthropic.com/pricing).
- **Ollama**: free, runs locally; no cloud cost.

## Disable AI assistant

To disable the AI panel:

1. Leave both API key env vars unset (the UI will show the configuration message), or  
2. Remove or comment out the `<AiChatPanel />` component in `client/src/pages/Topology.tsx`.
