# Kafka Topology API (Python backend)

FastAPI backend for the Kafka topology view app.

## Setup

```bash
cd server
uv sync   # or: pip install -e .
```

**Clusters** are stored in `data/clusters.json` (no database). Override path with `CLUSTERS_JSON`. See project README for cluster and optional SSL config.

Optional: `AI_INTEGRATIONS_OPENAI_API_KEY`, `AI_INTEGRATIONS_OPENAI_BASE_URL` for AI query.

**Topology** uses real cluster data: topics and consumer groups from the broker (`bootstrapServers`). If you set `connectUrl` (Kafka Connect REST) and/or `schemaRegistryUrl` (Confluent Schema Registry), connectors and schema subjects are included. Add a cluster with valid bootstrap servers to see real topics in the topology view.

## Run

```bash
uv run uvicorn main:app --reload --port 5000
```

Or: `uv run python main.py`
