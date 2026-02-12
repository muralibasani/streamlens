import os
import threading
import time
from contextlib import asynccontextmanager
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel

# Load environment variables from server directory so it works when run from project root
_server_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_server_dir, ".env.dev"))
load_dotenv(os.path.join(_server_dir, ".env"))

from storage import (
    get_clusters,
    get_cluster,
    create_cluster,
    update_cluster,
    delete_cluster,
    get_latest_snapshot,
    create_snapshot,
    sanitize_cluster_for_api,
)
from lib.topology import build_topology, paginate_topology_data, search_topology
from lib.ai import query_topology, get_ai_status


class CreateClusterBody(BaseModel):
    name: str
    bootstrapServers: str
    schemaRegistryUrl: str | None = None
    connectUrl: str | None = None
    jmxHost: str | None = None
    jmxPort: int | None = None
    enableKafkaEventProduceFromUi: bool | None = None


class AiQueryBody(BaseModel):
    question: str
    topology: dict


class ProduceMessageBody(BaseModel):
    value: str
    key: str | None = None


def indexer_loop():
    """Background loop: refresh topology snapshots every minute."""
    while True:
        time.sleep(60)
        try:
            clusters = get_clusters()
            for c in clusters:
                try:
                    print(f"Indexing cluster {c['name']}...")
                    graph = build_topology(c["id"], c)
                    create_snapshot(c["id"], graph)
                except Exception as e:
                    print(f"Indexer error for cluster {c['id']}: {e}")
        except Exception as e:
            print("Indexer error:", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from storage import _ensure_clusters_file
    _ensure_clusters_file()
    t = threading.Thread(target=indexer_loop, daemon=True)
    t.start()
    yield


app = FastAPI(title="Kafka Topology API", lifespan=lifespan)

# CORS for when frontend hits backend directly (e.g. production or VITE_API_URL)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/ai/status")
def ai_status():
    """Return the currently configured AI provider and model."""
    return get_ai_status()


@app.get("/api/clusters")
def clusters_list():
    return [sanitize_cluster_for_api(c) for c in get_clusters()]


@app.get("/api/clusters/{id}")
def clusters_get(id: int):
    cluster = get_cluster(id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return sanitize_cluster_for_api(cluster)


@app.get("/api/clusters/{id}/health")
def cluster_health(id: int):
    from lib.kafka import kafka_service

    cluster = get_cluster(id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    health = kafka_service.check_cluster_health(cluster)
    return {
        "clusterId": id,
        "online": health["online"],
        "error": health["error"],
        "clusterMode": health.get("clusterMode"),
    }


@app.post("/api/clusters")
def clusters_create(body: CreateClusterBody):
    try:
        cluster = create_cluster(
            name=body.name,
            bootstrap_servers=body.bootstrapServers,
            schema_registry_url=body.schemaRegistryUrl,
            connect_url=body.connectUrl,
            jmx_host=body.jmxHost,
            jmx_port=body.jmxPort,
            enable_kafka_event_produce_from_ui=body.enableKafkaEventProduceFromUi if body.enableKafkaEventProduceFromUi is not None else False,
        )
        graph = build_topology(cluster["id"], cluster)
        create_snapshot(cluster["id"], graph)
        return sanitize_cluster_for_api(cluster)
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail={"message": str(e), "field": None})
    except Exception as e:
        raise HTTPException(status_code=400, detail={"message": str(e), "field": None})


@app.put("/api/clusters/{id}")
def clusters_update(id: int, body: CreateClusterBody):
    existing = get_cluster(id)
    if not existing:
        raise HTTPException(status_code=404, detail="Cluster not found")
    jmx_host = body.jmxHost if body.jmxHost is not None else existing.get("jmxHost")
    jmx_port = body.jmxPort if body.jmxPort is not None else existing.get("jmxPort")
    cluster = update_cluster(
        id,
        name=body.name,
        bootstrap_servers=body.bootstrapServers,
        schema_registry_url=body.schemaRegistryUrl,
        connect_url=body.connectUrl,
        jmx_host=jmx_host,
        jmx_port=jmx_port,
        enable_kafka_event_produce_from_ui=body.enableKafkaEventProduceFromUi,  # None = preserve existing
    )
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return sanitize_cluster_for_api(cluster)

@app.delete("/api/clusters/{id}", status_code=204)
def clusters_delete(id: int):
    cluster = get_cluster(id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    delete_cluster(id)
    return Response(status_code=204)


@app.get("/api/clusters/{id}/topology")
def topology_get(id: int, topic_offset: int = 0, topic_limit: int = 0):
    """
    Return topology snapshot, optionally paginated.
    When topic_limit > 0, only a page of topics (sorted: connected first, then alphabetical)
    is returned along with their related non-topic nodes and edges.
    """
    cluster = get_cluster(id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    snapshot = get_latest_snapshot(id)
    if not snapshot:
        try:
            graph = build_topology(id, cluster)
            snapshot = create_snapshot(id, graph)
        except RuntimeError as e:
            raise HTTPException(status_code=502, detail=str(e))
        except Exception:
            raise HTTPException(status_code=404, detail="Topology not found")

    if topic_limit > 0 and isinstance(snapshot.get("data"), dict):
        paginated = paginate_topology_data(snapshot["data"], topic_offset, topic_limit)
        return {**snapshot, "data": paginated}
    return snapshot


@app.get("/api/clusters/{id}/topology/search")
def topology_search(id: int, q: str = ""):
    """
    Search ALL nodes in the full (unpaginated) topology snapshot.
    Returns matching nodes + their connected nodes/edges so the client
    can merge them into the visible graph.
    """
    cluster = get_cluster(id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    snapshot = get_latest_snapshot(id)
    if not snapshot or not isinstance(snapshot.get("data"), dict):
        raise HTTPException(status_code=404, detail="No topology snapshot available")
    return search_topology(snapshot["data"], q)


@app.post("/api/clusters/{id}/refresh")
def topology_refresh(id: int):
    cluster = get_cluster(id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    try:
        graph = build_topology(id, cluster)
        return create_snapshot(id, graph)
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/api/clusters/{id}/schema/{subject}")
def get_schema_details(id: int, subject: str, version: str | None = None):
    """
    Lazy-load schema details for a specific subject and version.
    If version is not provided, returns latest version.
    Also returns list of all available versions.
    Called on-demand when user clicks on a schema node.
    """
    from lib.kafka import kafka_service
    
    cluster = get_cluster(id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    schema_url = cluster.get("schemaRegistryUrl")
    if not schema_url:
        raise HTTPException(status_code=400, detail="Schema Registry not configured for this cluster")
    
    try:
        return kafka_service.fetch_schema_details(schema_url.rstrip("/"), subject, version)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/api/clusters/{id}/topic/{topic_name}/code")
def get_topic_code(
    id: int,
    topic_name: str,
    client: str = "producer",  # producer | consumer | streams
    language: str = "java",   # java | python (streams is Java only)
    schema_registry: bool = False,
    output_topic: str | None = None,  # for client=streams: target topic name
):
    """Generate sample producer/consumer/streams code for the topic. Users can copy the code."""
    from lib.codegen import generate_code

    cluster = get_cluster(id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    bootstrap_servers = cluster.get("bootstrapServers") or ""
    if not bootstrap_servers:
        raise HTTPException(status_code=400, detail="Bootstrap servers not configured")
    if client not in ("producer", "consumer", "streams"):
        raise HTTPException(status_code=400, detail="client must be producer, consumer, or streams")
    if client != "streams" and language not in ("java", "python"):
        raise HTTPException(status_code=400, detail="language must be java or python")

    if client == "streams":
        code = generate_code(
            bootstrap_servers=bootstrap_servers,
            topic=topic_name,
            client="streams",
            language="java",
            schema_registry=False,
            output_topic=output_topic or None,
        )
        return {"code": code, "client": "streams", "language": "java", "schemaRegistry": False}
    schema_registry_url = cluster.get("schemaRegistryUrl") if schema_registry else None
    code = generate_code(
        bootstrap_servers=bootstrap_servers,
        topic=topic_name,
        client=client,
        language=language,
        schema_registry=schema_registry,
        schema_registry_url=schema_registry_url,
    )
    return {"code": code, "client": client, "language": language, "schemaRegistry": schema_registry}


@app.get("/api/clusters/{id}/topic/{topic_name}/details")
def get_topic_details(id: int, topic_name: str, include_messages: bool = False):
    """
    Fetch detailed configuration for a specific topic.
    Optionally fetch recent messages if include_messages=true.
    Called on-demand when user clicks on a topic node.
    """
    from lib.kafka import kafka_service
    
    cluster = get_cluster(id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    if not cluster.get("bootstrapServers"):
        raise HTTPException(status_code=400, detail="Bootstrap servers not configured")
    
    try:
        return kafka_service.fetch_topic_details(cluster, topic_name, include_messages)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/api/clusters/{id}/topic/{topic_name}/produce")
def produce_to_topic(id: int, topic_name: str, body: ProduceMessageBody):
    """
    Produce a single message to a topic. Value is free text; key is optional.
    Enabled only when ENABLE_KAFKA_EVENT_PRODUCE_FROM_UI is set (true/1/yes).
    """
    from lib.kafka import kafka_service

    cluster = get_cluster(id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    if not cluster.get("enableKafkaEventProduceFromUi"):
        raise HTTPException(
            status_code=403,
            detail="Produce from UI is disabled for this cluster. Enable it in cluster settings (edit cluster).",
        )

    # Disallow produce to topics that have a connector attached
    snapshot = get_latest_snapshot(id)
    if snapshot and isinstance(snapshot.get("data"), dict):
        edges = snapshot["data"].get("edges") or []
        topic_id = f"topic:{topic_name}"
        for e in edges:
            src, tgt = str(e.get("source") or ""), str(e.get("target") or "")
            if (src == topic_id and tgt.startswith("connect:")) or (
                tgt == topic_id and src.startswith("connect:")
            ):
                raise HTTPException(
                    status_code=403,
                    detail="Cannot produce to topics that have connectors attached.",
                )

    if not cluster.get("bootstrapServers"):
        raise HTTPException(status_code=400, detail="Bootstrap servers not configured")

    try:
        return kafka_service.produce_message(cluster, topic_name, body.value, body.key)
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/clusters/{id}/connector/{connector_name}/details")
def get_connector_details(id: int, connector_name: str):
    """
    Fetch detailed configuration for a specific connector.
    Sensitive values are masked.
    Called on-demand when user clicks on a connector node.
    """
    from lib.kafka import kafka_service
    
    cluster = get_cluster(id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    connect_url = cluster.get("connectUrl")
    if not connect_url:
        raise HTTPException(status_code=400, detail="Kafka Connect URL not configured for this cluster")
    
    try:
        # Strip "connect:" prefix if present (node ID format)
        clean_name = connector_name.replace("connect:", "", 1) if connector_name.startswith("connect:") else connector_name
        return kafka_service.fetch_connector_details(connect_url.rstrip("/"), clean_name)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/api/clusters/{id}/consumer/{group_id}/lag")
def get_consumer_lag(id: int, group_id: str):
    """
    Fetch consumer lag per partition for a specific consumer group.
    Called on-demand when user clicks on a consumer node.
    """
    from lib.kafka import kafka_service
    
    cluster = get_cluster(id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    bootstrap_servers = cluster.get("bootstrapServers")
    if not bootstrap_servers:
        raise HTTPException(status_code=400, detail="Bootstrap servers not configured")
    
    try:
        return kafka_service.fetch_consumer_lag(cluster, group_id)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/api/ai/query")
def ai_query(body: AiQueryBody):
    try:
        return query_topology(body.question, body.topology)
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="AI processing failed")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "5000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
