# Kafka Topology View

A full-stack app for visualizing Kafka topologies (topics, producers, consumers, streams, connectors) with an AI Q&A layer.

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

## Fully Auto-Discovered

All entities are automatically discovered in real-time - no manual configuration needed!

### Visual Indicators

- **🌟 Live** (Green badge) — Consumers auto-discovered from Kafka consumer groups in real-time
- **⚡ JMX** (Yellow badge) — Active producers detected from JMX metrics (topics receiving messages NOW)
- **🛡️ ACL** (Amber badge) — Potential producers derived from Kafka ACLs (WRITE permissions)

Click the **ℹ️ Info** button in the topology view for more details.

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

# 2. Install JMX library
cd server
pip install jmxquery

# 3. Configure cluster with JMX (via UI or API)
# Add JMX Host: localhost, JMX Port: 9999

# 4. Click Sync in UI → See ⚡ JMX producers!
```
