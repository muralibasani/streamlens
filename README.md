<p align="center">
  <img src="client/public/favicon.svg" alt="StreamLens logo" width="100" />
</p>

<h1 align="center">streamLens</h1>

<p align="center">
  <strong>Visualize and explore Apache Kafka topologies — topics, producers, consumers, streams, schemas, connectors, and ACLs — with an AI assistant that queries live broker metrics.</strong>
</p>

<p align="center">
  <a href="https://github.com/muralibasani/streamlens/stargazers"><img src="https://img.shields.io/github/stars/muralibasani/streamlens?style=flat&logo=github&label=Stars" alt="GitHub stars" /></a>
  <a href="https://github.com/muralibasani/streamlens/blob/main/LICENSE"><img src="https://img.shields.io/github/license/muralibasani/streamlens?style=flat" alt="License" /></a>
  <a href="https://github.com/muralibasani/streamlens/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/muralibasani/streamlens/ci.yml?branch=main&label=CI" alt="CI" /></a>
  <a href="https://github.com/muralibasani/streamlens/pulls"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" alt="PRs Welcome" /></a>
</p>

<p align="center">
  <img src="resources/StreamLens.gif" alt="StreamLens" width="700" />
</p>

<p align="center">
  <a href="https://www.youtube.com/watch?v=lQIdaqVqgtk">Watch the full setup and demo on YouTube</a>
</p>

## Features

- **Live topology visualization** — Interactive graph of your Kafka cluster powered by React Flow
- **Auto-discovery** — Topics, consumer groups, producers, connectors, schemas, and ACLs are detected automatically from the live cluster
- **Schema grouping** — Schemas sharing the same Schema Registry ID are merged into a single node connected to all related topics
- **Consumer lag** — Click any consumer node to view per-partition lag
- **Topic details** — Click any topic node to view configuration, recent messages, and generate sample client code (Java / Python)
- **Connector details** — Click connector nodes to inspect configuration (sensitive values masked)
- **Producer detection** — Via JMX metrics or offset-change detection (automatic fallback chain)
- **Search & navigation** — Find nodes by name or type, auto-zoom to matches, keyboard navigation (Enter / Shift+Enter)
- **Topic pagination** — Large clusters load incrementally (connected topics first), with search across all topics
- **Produce messages** — Optionally produce messages from the UI (disabled by default, per-cluster opt-in)
- **AI assistant (StreamPilot)** — Ask questions about your topology and live broker metrics; highlights and zooms to relevant nodes. Supports OpenAI, Gemini, Anthropic, and Ollama
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
      "connectUrl": "http://localhost:8083",
      "prometheusUrl": "http://prometheus:9090",
      "jmxHost": "localhost",
      "jmxPort": 9999
    }
  ]
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `bootstrapServers` | Yes | Kafka broker address(es) |
| `schemaRegistryUrl` | No | Schema Registry URL (enables schema nodes) |
| `connectUrl` | No | Kafka Connect REST URL (enables connector nodes) |
| `prometheusUrl` | No | Prometheus URL — enables AI-powered broker metric queries and producer detection per client ID. **If set, `jmxHost`/`jmxPort` are not required.** |
| `jmxHost` / `jmxPort` | No | Broker JMX endpoint — fallback for producer detection when Prometheus is not available |
| `enableKafkaEventProduceFromUi` | No | Allow producing messages from the UI (default: `false`) |

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

### Producer Detection

StreamLens detects producers using an automatic fallback chain. Only the first source that returns results is used:

| Priority | Source | Granularity | Config field |
|----------|--------|-------------|--------------|
| 1 | **Prometheus (client-side)** | Per client ID + topic | `prometheusUrl` |
| 2 | **Prometheus (broker-side)** | Per topic (aggregate) | `prometheusUrl` |
| 3 | **Broker JMX** | Per topic (aggregate) | `jmxHost` + `jmxPort` |
| 4 | **Offset change** | Per topic (needs 2 syncs) | _(automatic)_ |

> **Tip:** If `prometheusUrl` is configured, `jmxHost` and `jmxPort` are not required. Prometheus covers both client-side (per producer client ID) and broker-side (per topic) detection, plus AI-powered broker metric queries.

**Prometheus (recommended)** — Run the [JMX Exporter](https://github.com/prometheus/jmx_exporter) as a Java agent on the **Kafka brokers** (for broker-side producer detection and AI metric queries) and optionally on **producer applications** (for per-client-id detection). StreamLens first tries `kafka_producer_topic_metrics_record_send_total` grouped by `client_id` and `topic`. If no client-side metrics are found, it falls back to broker-side `kafka_server_brokertopicmetrics_messagesinpersec` per topic — no JMX port needed for either.

**Broker JMX** — Falls back to broker-side `MessagesInPerSec` metrics (one producer node per active topic, no client IDs). Only needed if Prometheus is not configured:

1. Start Kafka with `JMX_PORT=9999`
2. Set `jmxHost` and `jmxPort` in the cluster config
3. Restart the backend and click Sync

### AI Assistant (Optional)

See [docs/AI_SETUP.md](docs/AI_SETUP.md) for configuring the StreamPilot AI chat (OpenAI, Gemini, Anthropic, or Ollama).

**AI Broker Metrics** — When `prometheusUrl` is configured and the JMX Exporter is running on the Kafka brokers, the AI assistant can answer questions about live **broker metrics**. Ask questions like:

- "What is the current message throughput?"
- "Are there any under-replicated partitions?"
- "What is the avg time to handle a produce request?"
- "How many partitions do we have?"
- "Show me all metrics"

StreamLens queries a curated set of 17 broker metrics from Prometheus across 5 categories:

| Category | Example metrics |
|----------|----------------|
| Cluster Health | Under-replicated partitions, active controller count, offline partitions |
| Throughput | Messages in/sec (total and per topic), bytes in/out per sec |
| Request Performance | Produce/fetch request rate, produce/fetch avg latency |
| Broker Resources | Partition count, leader count, log size |
| Replication | ISR shrinks/expands per sec |

See `server/src/kafka/metrics.py` for the full catalog and PromQL queries.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `CLUSTERS_JSON` | Path to clusters file (default: `server/data/clusters.json`) |
| `TOPOLOGY_MAX_TOPICS` | Max topic nodes per snapshot (default: `2000`) |
| `VITE_API_URL` | Frontend API target (default: `http://localhost:5000`) |
| `AI_PROVIDER` | AI provider: `openai`, `gemini`, `anthropic`, `ollama` |

See [docs/AI_SETUP.md](docs/AI_SETUP.md) for AI-specific env vars.

## Support

If you find StreamLens useful, consider giving it a star on GitHub — it helps others discover the project and motivates continued development.

<a href="https://github.com/muralibasani/streamlens/stargazers"><img src="https://img.shields.io/github/stars/muralibasani/streamlens?style=social" alt="Star on GitHub" /></a>

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
