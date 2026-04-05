# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is StreamLens

StreamLens is a Kafka topology visualization and exploration tool. It auto-discovers topics, consumer groups, producers, connectors, schemas, and ACLs from live Kafka clusters and renders them as an interactive graph. It includes an AI assistant (StreamPilot) that can query live broker metrics via Prometheus.

## Architecture

**Monorepo with two main components:**

- **`server/`** — Python (FastAPI) backend using `uv` for dependency management. Entry point is `server/main.py`. Source code lives in `server/src/` with Kafka-specific modules under `server/src/kafka/`. Cluster configs are stored in `server/data/clusters.json` (no database). A background thread refreshes topology snapshots every 60 seconds.
- **`client/`** — React 18 + TypeScript frontend using Vite, Tailwind CSS, and shadcn/ui components. Uses `wouter` for routing, `reactflow` for the graph visualization, `@tanstack/react-query` for data fetching, and `dagre` for graph layout. Path alias `@/` maps to `client/src/`.

**Key server modules:**
- `src/topology.py` — Builds the full topology graph (nodes + edges) and handles pagination/search
- `src/ai.py` — AI assistant integration (OpenAI, Gemini, Anthropic, Ollama)
- `src/storage.py` — Cluster CRUD and snapshot persistence (JSON file-based)
- `src/codegen.py` — Sample code generation (Java/Python producer/consumer/streams)
- `src/kafka/service.py` — Core Kafka operations (topic details, consumer lag, connector details, schema details, producer detection)
- `src/kafka/metrics.py` — Prometheus metric queries for broker metrics
- `src/kafka/producers.py` — Producer detection fallback chain (Prometheus → JMX → offset change)

**Key client structure:**
- `src/pages/` — `Dashboard.tsx` (cluster list), `Topology.tsx` (main graph view)
- `src/components/` — `TopologyNode.tsx`, `AiChatPanel.tsx`, `StreamsEdge.tsx`
- `src/hooks/use-kafka.ts` — React Query hooks for API calls
- `src/lib/api.ts` — API client functions

## Common Commands

### Install dependencies
```bash
make install                    # both server + client
cd server && uv sync --extra dev  # server only (with test deps)
cd client && npm install          # client only
```

### Run locally (development)
```bash
# Backend (from server/)
uv run uvicorn main:app --reload --port 5000

# Frontend (from client/)
npm run dev    # opens http://localhost:5173, proxies /api to :5000
```

### Run tests
```bash
make test                # all unit tests (server + client)
make test-server         # server unit tests: uv run pytest tests/unit -v
make test-client         # client tests: npm test (vitest)

# Single server test file
cd server && uv run pytest tests/unit/test_topology.py -v

# Single client test file
cd client && npx vitest run src/components/TopologyNode.test.tsx

# Integration tests (starts Kafka via Docker)
make test-integration
```

### Type checking and build
```bash
make typecheck           # TypeScript: cd client && npx tsc --noEmit
cd client && npm run build  # production build
```

### Docker
```bash
docker build -f container/Dockerfile -t streamlens .
docker run -p 5000:5000 streamlens

# Start/stop test infrastructure (Kafka + Schema Registry + Connect)
make docker-up
make docker-down
```

### Full CI check (mirrors GitHub Actions)
```bash
make ci    # runs: test → typecheck → test-integration → client build
```

## Conventions

- **Commits**: Follow [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `docs:`, `refactor:`, `chore:`. Imperative mood, under 72 chars.
- **Python**: Always use `uv run` to execute Python commands (ensures correct venv). Python 3.11+.
- **Server env files**: `server/.env.dev` and `server/.env` are loaded automatically by `main.py`.
- **Client path alias**: Use `@/` to import from `client/src/` (configured in `vite.config.ts` and `tsconfig.json`).
- **Testing**: Server uses pytest; client uses vitest with jsdom + React Testing Library.

## Development Guidelines

### State management in async contexts
When state is read inside `useCallback`, `useEffect`, or async callbacks (debounced functions, promises, timeouts), verify it won't be stale at execution time. Either add it to dependency arrays or use a ref that syncs on every render. When a user action changes state that in-flight async work depends on, cancel pending work first (clear timeouts, abort fetches, reset flags) before applying the new state.

### Testing practices
- Tests must always import and exercise the production implementation. Never copy production logic into test files — if something isn't exported, extract it to a shared module under `client/src/lib/` and import it in both production code and tests.
- Place unit tests adjacent to the module they test (e.g., `filterGraph.ts` → `filterGraph.test.ts`). Page/component integration tests go alongside the component (e.g., `Topology.test.tsx`).
