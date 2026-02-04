# StreamLens - Apache Kafka Topology View

A full-stack app for visualizing Kafka topologies (topics, producers, consumers, streams, schemas, connectors) with an AI Q&A layer.

## Project structure

- **client/** — React frontend (Vite, TypeScript). Dependencies: `client/package.json`.
- **server/** — Python backend (FastAPI). Dependencies: `server/pyproject.toml`.

## Quick start

**Backend**

```bash
cd server
uv sync   # or: pip install -e .
uv run uvicorn main:app --reload --port 5000
```

**Frontend**

```bash
cd client
npm install
npm run dev
```

Open the client (e.g. http://localhost:5173). In dev, the client proxies `/api` to the backend (default `http://localhost:5000`). Set `VITE_API_URL` if your API runs elsewhere.

## Security & Permissions

### Kafka ACLs (Required for secured clusters)

If your Kafka cluster has ACLs enabled, you need to grant the StreamLens application READ permissions to access cluster metadata:

```bash
# Grant READ access to topics, consumer groups, and cluster metadata
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:streamlens \
  --operation Read --topic '*' \
  --operation Describe --topic '*' \
  --operation Describe --cluster \
  --operation Describe --group '*'

# For JMX producer detection (optional), ensure JMX ports are accessible
# No additional Kafka ACLs needed for JMX
```

**Replace `User:streamlens` with your actual principal** (SASL username, mTLS DN, etc.)

### Read-Only Tool

**StreamLens is 100% read-only.** It only performs the following operations:
- ✅ List topics, consumer groups, connectors, schemas
- ✅ Read cluster metadata and metrics
- ✅ Query JMX for producer detection (if enabled)

**It NEVER:**
- ❌ Creates, updates, or deletes topics
- ❌ Modifies consumer group offsets
- ❌ Produces or consumes messages
- ❌ Changes any cluster configuration

This makes StreamLens safe to use in production environments for monitoring and visualization.

## Environment

- **server**: `DATABASE_URL` (optional; default: SQLite at `server/topology.db`; use `postgresql://...` and `uv sync --extra postgres` to switch to PostgreSQL). Optional: `AI_INTEGRATIONS_OPENAI_API_KEY`, `AI_INTEGRATIONS_OPENAI_BASE_URL` for AI query.
- **client**: `VITE_API_URL` (optional, default `http://localhost:5000` for proxy target).

## Topology: Auto-Discovery & Manual Registration

### Auto-Discovered Entities (Real-Time, No Client Changes)

- **Topics** — Fetched from Kafka broker metadata
- **Consumer Groups** — Auto-discovered via AdminClient (shows which topics each group consumes from)
- **Producers (JMX)** — Optional: If JMX enabled on brokers, shows topics with active producers RIGHT NOW (see [JMX Setup](docs/JMX_SETUP.md))
- **Producers (ACL)** — Optional: If Kafka ACLs are enabled, shows potential producers based on WRITE permissions
- **Connectors** — Fetched from Kafka Connect REST API (if configured)
- **Schemas** — Fetched from Schema Registry REST API (if configured)

## Auto-Discovery

Most entities are automatically discovered in real-time:
- **Topics** — From Kafka broker
- **Consumer Groups** — From Kafka AdminClient
- **Producers** — From JMX metrics or ACLs
- **Connectors** — From Kafka Connect API (optional)
- **Schemas** — From Schema Registry (optional, click schema nodes to view full definitions)

### Kafka Streams Configuration

For **Kafka Streams applications**, create a simple `server/streams.yaml` file to link input and output topics:

```yaml
streams:
  - name: payment-processor
    consumerGroup: payment-processor-app
    inputTopics: [payments.raw]
    outputTopics: [payments.processed]
```

See [`docs/STREAMS_CONFIG.md`](docs/STREAMS_CONFIG.md) for full documentation.

### Visual Indicators

- **🌟 Live** (Green badge) — Consumers auto-discovered from Kafka consumer groups in real-time
- **⚡ JMX** (Yellow badge) — Active producers detected from JMX metrics (topics receiving messages NOW)
- **📄 Schema Nodes** — Topics with registered schemas show small linked schema nodes. **Click the schema node** to view the full schema definition in a dialog.

**To enable Schema Registry integration**: When adding/editing a cluster, provide the `schemaRegistryUrl` (e.g., `http://localhost:8081`). Schema nodes will appear automatically after refreshing the topology.

Click the **ℹ️ Info** button in the topology view for more details.

### StreamPilot AI Assistant

The topology includes an AI-powered chat assistant (**StreamPilot**) that can answer questions about your Kafka cluster and **automatically highlight & zoom to relevant nodes**.

**Setup Required**: See [`docs/AI_SETUP.md`](docs/AI_SETUP.md) for configuration instructions.

Example questions:
- "Which producers write to testtopic?" → Highlights producers & zooms to them
- "Show me all consumers for orders topic" → Highlights consumers & focuses view
- "What topics does my-app produce to?" → Shows producing relationships with zoom

### Search & Navigation

- **Search Box** — Find nodes by name, type, or ID (topics, producers, consumers, etc.)
- **Auto-Zoom** — Automatically centers and zooms to matching nodes
- **Multiple Matches** — Navigate through results with ↑/↓ buttons or keyboard:
  - `Enter` — Next match
  - `Shift+Enter` — Previous match
- **Highlighted Results** — Matching nodes are highlighted with a yellow ring

## JMX Producer Auto-Discovery (Optional)

Enable JMX on your Kafka brokers to see real-time active producers. See **[docs/JMX_SETUP.md](docs/JMX_SETUP.md)** for complete setup guide.

**Quick Start:**
```bash
# 1. Enable JMX on Kafka broker
export JMX_PORT=9999
./bin/kafka-server-start.sh config/server.properties


```
