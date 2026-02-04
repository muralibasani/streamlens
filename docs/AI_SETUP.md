# AI Chat Assistant Setup

The topology view includes an AI-powered chat assistant that can answer questions about your Kafka cluster.

## Prerequisites

The AI assistant uses OpenAI's API by default. You'll need:
- An OpenAI API key (get one at: https://platform.openai.com/api-keys)
- The `openai` Python package (already in dependencies)

## Configuration

### 1. Set Environment Variables

Create a `.env` file in the `server/` directory:

```bash
cd server
cp .env.example .env
```

Edit `.env` and add your OpenAI API key:

```bash
AI_INTEGRATIONS_OPENAI_API_KEY=sk-your-actual-api-key-here
```

### 2. Load Environment Variables

Make sure your server loads the `.env` file. You can use `python-dotenv`:

```bash
cd server
uv add python-dotenv
```

Then update `server/main.py` to load the `.env` file:

```python
from dotenv import load_dotenv

load_dotenv()  # Load .env file at startup
```

### 3. Restart the Server

```bash
cd server
uv run python main.py
```

## Alternative: Use Local LLM or Azure OpenAI

If you want to use a different LLM provider:

### Local LLM (Ollama, LM Studio, etc.)

```bash
AI_INTEGRATIONS_OPENAI_API_KEY=dummy
AI_INTEGRATIONS_OPENAI_BASE_URL=http://localhost:11434/v1
```

Make sure your local LLM is running and supports OpenAI-compatible API.

### Azure OpenAI

```bash
AI_INTEGRATIONS_OPENAI_API_KEY=your-azure-api-key
AI_INTEGRATIONS_OPENAI_BASE_URL=https://your-resource.openai.azure.com/
```

## Troubleshooting

### Error: "I couldn't process your request at the moment"

**Cause**: API key not configured or invalid.

**Solution**:
1. Check your `.env` file exists in `server/`
2. Verify the API key is correct
3. Check server logs for the actual error:
   ```bash
   cd server
   uv run python main.py
   ```
   Look for "AI Query failed" in the output

### Error: "AI Query failed"

Check the backend console logs for the specific error. Common issues:
- Invalid API key
- Network connectivity problems
- Rate limits exceeded
- Base URL misconfigured

## Testing

Once configured, try asking questions like:
- "Which producers write to testtopic?"
- "Show me all consumers for orders topic"
- "What topics does my-app produce to?"

The assistant will analyze your topology graph and provide answers with visual highlights.

## Cost Considerations

The AI assistant uses OpenAI's `gpt-4o-mini` model, which is cost-effective:
- ~$0.15 per 1M input tokens
- ~$0.60 per 1M output tokens

Each query typically uses 500-2000 tokens depending on your topology size.

## Disable AI Assistant (Optional)

If you don't want to use the AI assistant, you can:
1. Leave the environment variables unset (it will gracefully fail with the error message)
2. Or hide the AI panel from the UI by commenting out the `<AiChatPanel />` component in `client/src/pages/Topology.tsx`
