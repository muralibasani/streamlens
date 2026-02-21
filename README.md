<p align="center">
  <img src="client/public/favicon.svg" alt="StreamLens logo" width="100" />
</p>

<h1 align="center">streamLens</h1>

<p align="center">A full-stack app for visualizing Apache Kafka topologies — topics, producers, consumers, streams, schemas, connectors, and ACLs — with an optional AI assistant.</p>

<p align="center">
  <img src="resources/StreamLens.gif" alt="StreamLens" width="700" />
</p>

## Features

- **Live topology visualization** — Interactive graph of your Kafka cluster powered by React Flow
- **Auto-discovery** — Topics, consumer groups, producers, connectors, schemas, and ACLs are detected automatically from the live cluster
- **Schema grouping** — Schemas sharing the same Schema Registry ID are merged into a single node connected to all related topics
- **Consumer lag** — Click any consumer node to view per-partition lag
- **Topic details** — Click any topic node to view configuration, recent messages, and generate sample client code (Java / Python)
- **Connector details** — Click connector nodes to inspect configuration (sensitive values masked)
- **Kafka Streams** — Visualize stream processing pipelines (input → output) via `streams.yaml`
- **Producer detection** — Via JMX metrics, ACL WRITE permissions, or offset-change detection
- **Search & navigation** — Find nodes by name or type, auto-zoom to matches, keyboard navigation (Enter / Shift+Enter)
- **Topic pagination** — Large clusters load incrementally (connected topics first), with search across all topics
- **Produce messages** — Optionally produce messages from the UI (disabled by default, per-cluster opt-in)
- **AI assistant (StreamPilot)** — Ask questions about your topology; highlights and zooms to relevant nodes. Supports OpenAI, Gemini, Anthropic, and Ollama
- **Dark / light theme** — Toggle between themes

## Quick Start

### Local development

**Backend**

```bash
cd server
uv sync
uv run uvicorn main:app --reload --port 5000
```

> Always use `uv run` to ensure the correct environment is used.

**Frontend**

```bash
cd client
npm install
npm run dev
```

Open http://localhost:5173. The dev server proxies `/api` to the backend. Set `VITE_API_URL` if your backend runs on a different host/port.

### Docker

```bash
docker build -f container/Dockerfile -t streamlens .
docker run -p 5000:5000 streamlens
```

Open http://localhost:5000. See [container/README.md](container/README.md) for volume mounts and environment variables.

## Project Structure

- **client/** — React frontend (Vite, TypeScript, Tailwind, shadcn/ui)
- **server/** — Python backend (FastAPI)
- **container/** — Dockerfile and Docker Compose for deployment and testing
- **docs/** — Additional documentation ([AI setup](docs/AI_SETUP.md), [topology](docs/TOPOLOGY.md))

## Configuration

### Clusters

Clusters are stored in `server/data/clusters.json` (no database required). Add clusters through the UI, or edit the file directly and restart the server.

```json
{
  "clusters": [
    {
      "id": 1,
      "name": "My Cluster",
      "bootstrapServers": "localhost:9092",
      "schemaRegistryUrl": "http://localhost:8081",
      "connectUrl": "http://localhost:8083"
    }
  ]
}
```

Override the file path with the `CLUSTERS_JSON` env var.

### Supported Protocols

streamLens currently supports **PLAINTEXT** and **SSL** Kafka listener protocols.

For SSL connections, add these fields to the cluster object:

- `securityProtocol` — `"SSL"` or `"PLAINTEXT"` (default)
- `sslEndpointIdentificationAlgorithm` — `""` to disable hostname verification (dev/self-signed)
- **PEM paths:** `sslCaLocation`, `sslCertificateLocation`, `sslKeyLocation`, `sslKeyPassword`
- **Java truststore/keystore** (auto-converted to PEM; requires `keytool` + `openssl`): `sslTruststoreLocation`, `sslTruststorePassword`, `sslKeystoreLocation`, `sslKeystoreType`, `sslKeystorePassword`, `sslKeyPassword`
- `enableSslCertificateVerification` — `false` to skip broker cert verification (insecure, dev only)

### Kafka ACLs

If your cluster has ACLs enabled, grant StreamLens READ/Describe permissions:

```bash
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:streamlens --allow-host streamlenshost \
  --operation Read --topic '*' \
  --operation Describe --topic '*' \
  --operation Describe --cluster \
  --operation Describe --group '*'
```

Replace `User:streamlens` with your actual principal.

### JMX Producer Detection (Optional)

Enable JMX on your Kafka brokers to detect active producers in real-time:

1. Start Kafka with `JMX_PORT=9999`
2. Set `jmxHost` and `jmxPort` in the cluster config (or use `server/debug/configure_jmx.py`)
3. Restart the backend and click Sync

### AI Assistant (Optional)

See [docs/AI_SETUP.md](docs/AI_SETUP.md) for configuring the StreamPilot AI chat (OpenAI, Gemini, Anthropic, or Ollama).

## Environment Variables

| Variable | Description |
|----------|-------------|
| `CLUSTERS_JSON` | Path to clusters file (default: `server/data/clusters.json`) |
| `TOPOLOGY_MAX_TOPICS` | Max topic nodes per snapshot (default: `2000`) |
| `VITE_API_URL` | Frontend API target (default: `http://localhost:5000`) |
| `AI_PROVIDER` | AI provider: `openai`, `gemini`, `anthropic`, `ollama` |

See [docs/AI_SETUP.md](docs/AI_SETUP.md) for AI-specific env vars.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
