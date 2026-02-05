import os
import threading
import time
from contextlib import asynccontextmanager
from dotenv import load_dotenv

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
from sqlalchemy.orm import Session

# Load environment variables from server directory so it works when run from project root
_server_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_server_dir, ".env.dev"))
load_dotenv(os.path.join(_server_dir, ".env"))

from db import get_db, init_db
from storage import (
    get_clusters,
    get_cluster,
    create_cluster as db_create_cluster,
    delete_cluster as db_delete_cluster,
    get_latest_snapshot,
    create_snapshot,
)
from lib.topology import build_topology
from lib.ai import query_topology


class CreateClusterBody(BaseModel):
    name: str
    bootstrapServers: str
    schemaRegistryUrl: str | None = None
    connectUrl: str | None = None
    jmxHost: str | None = None
    jmxPort: int | None = None


class AiQueryBody(BaseModel):
    question: str
    topology: dict


class ProduceMessageBody(BaseModel):
    value: str
    key: str | None = None


def seed_data(db: Session):
    clusters = get_clusters(db)
    if not clusters:
        db_create_cluster(
            db,
            name="Production Cluster",
            bootstrap_servers="pkc-1234.us-east-1.aws.confluent.cloud:9092",
            schema_registry_url="https://psrc-1234.us-east-1.aws.confluent.cloud",
            connect_url="https://connect.internal:8083",
        )


def indexer_loop():
    """Background loop: refresh topology snapshots every minute."""
    from db import SessionLocal
    while True:
        time.sleep(60)
        try:
            db = SessionLocal()
            clusters = get_clusters(db)
            for c in clusters:
                try:
                    print(f"Indexing cluster {c['name']}...")
                    graph = build_topology(c["id"], c)
                    create_snapshot(db, c["id"], graph)
                except Exception as e:
                    print(f"Indexer error for cluster {c['id']}: {e}")
            db.close()
        except Exception as e:
            print("Indexer error:", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    from db import SessionLocal
    db = SessionLocal()
    seed_data(db)
    db.close()
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


@app.get("/api/clusters")
def clusters_list(db: Session = Depends(get_db)):
    return get_clusters(db)


def _is_produce_from_ui_enabled() -> bool:
    return os.environ.get("ENABLE_KAFKA_EVENT_PRODUCE_FROM_UI", "false").strip().lower() in ("true", "1", "yes")


@app.get("/api/clusters/{id}")
def clusters_get(id: int, db: Session = Depends(get_db)):
    cluster = get_cluster(db, id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return {**cluster, "enableKafkaEventProduceFromUi": _is_produce_from_ui_enabled()}


@app.get("/api/clusters/{id}/health")
def cluster_health(id: int, db: Session = Depends(get_db)):
    from lib.kafka import kafka_service
    
    cluster = get_cluster(db, id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    health = kafka_service.check_cluster_health(cluster.get("bootstrapServers", ""))
    return {
        "clusterId": id,
        "online": health["online"],
        "error": health["error"],
        "clusterMode": health.get("clusterMode"),
    }


@app.post("/api/clusters")
def clusters_create(body: CreateClusterBody, db: Session = Depends(get_db)):
    try:
        cluster = db_create_cluster(
            db,
            name=body.name,
            bootstrap_servers=body.bootstrapServers,
            schema_registry_url=body.schemaRegistryUrl,
            connect_url=body.connectUrl,
            jmx_host=body.jmxHost,
            jmx_port=body.jmxPort,
        )
        graph = build_topology(cluster["id"], cluster)
        create_snapshot(db, cluster["id"], graph)
        return cluster
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail={"message": str(e), "field": None})
    except Exception as e:
        raise HTTPException(status_code=400, detail={"message": str(e), "field": None})


@app.put("/api/clusters/{id}")
def clusters_update(id: int, body: CreateClusterBody, db: Session = Depends(get_db)):
    from storage import update_cluster as db_update_cluster
    existing = get_cluster(db, id)
    if not existing:
        raise HTTPException(status_code=404, detail="Cluster not found")
    # Preserve JMX if not sent (e.g. Edit form didn't include them)
    jmx_host = body.jmxHost if body.jmxHost is not None else existing.get("jmxHost")
    jmx_port = body.jmxPort if body.jmxPort is not None else existing.get("jmxPort")
    cluster = db_update_cluster(
        db,
        id,
        name=body.name,
        bootstrap_servers=body.bootstrapServers,
        schema_registry_url=body.schemaRegistryUrl,
        connect_url=body.connectUrl,
        jmx_host=jmx_host,
        jmx_port=jmx_port,
    )
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return cluster

@app.delete("/api/clusters/{id}", status_code=204)
def clusters_delete(id: int, db: Session = Depends(get_db)):
    db_delete_cluster(db, id)
    return Response(status_code=204)


@app.get("/api/clusters/{id}/topology")
def topology_get(id: int, db: Session = Depends(get_db)):
    cluster = get_cluster(db, id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    snapshot = get_latest_snapshot(db, id)
    if snapshot:
        return snapshot
    try:
        graph = build_topology(id, cluster)
        snapshot = create_snapshot(db, id, graph)
        return snapshot
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception:
        raise HTTPException(status_code=404, detail="Topology not found")


@app.post("/api/clusters/{id}/refresh")
def topology_refresh(id: int, db: Session = Depends(get_db)):
    cluster = get_cluster(db, id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    try:
        graph = build_topology(id, cluster)
        return create_snapshot(db, id, graph)
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/api/clusters/{id}/schema/{subject}")
def get_schema_details(id: int, subject: str, version: str | None = None, db: Session = Depends(get_db)):
    """
    Lazy-load schema details for a specific subject and version.
    If version is not provided, returns latest version.
    Also returns list of all available versions.
    Called on-demand when user clicks on a schema node.
    """
    from lib.kafka import kafka_service
    
    cluster = get_cluster(db, id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    schema_url = cluster.get("schemaRegistryUrl")
    if not schema_url:
        raise HTTPException(status_code=400, detail="Schema Registry not configured for this cluster")
    
    try:
        return kafka_service.fetch_schema_details(schema_url.rstrip("/"), subject, version)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/api/clusters/{id}/topic/{topic_name}/details")
def get_topic_details(id: int, topic_name: str, include_messages: bool = False, db: Session = Depends(get_db)):
    """
    Fetch detailed configuration for a specific topic.
    Optionally fetch recent messages if include_messages=true.
    Called on-demand when user clicks on a topic node.
    """
    from lib.kafka import kafka_service
    
    cluster = get_cluster(db, id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    bootstrap_servers = cluster.get("bootstrapServers")
    if not bootstrap_servers:
        raise HTTPException(status_code=400, detail="Bootstrap servers not configured")
    
    try:
        return kafka_service.fetch_topic_details(bootstrap_servers, topic_name, include_messages)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/api/clusters/{id}/topic/{topic_name}/produce")
def produce_to_topic(
    id: int,
    topic_name: str,
    body: ProduceMessageBody,
    db: Session = Depends(get_db),
):
    """
    Produce a single message to a topic. Value is free text; key is optional.
    Enabled only when ENABLE_KAFKA_EVENT_PRODUCE_FROM_UI is set (true/1/yes).
    """
    from lib.kafka import kafka_service

    if not _is_produce_from_ui_enabled():
        raise HTTPException(
            status_code=403,
            detail="Produce from UI is disabled. Set ENABLE_KAFKA_EVENT_PRODUCE_FROM_UI=true to enable.",
        )

    cluster = get_cluster(db, id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")

    # Disallow produce to topics that have a connector attached
    snapshot = get_latest_snapshot(db, id)
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

    bootstrap_servers = cluster.get("bootstrapServers")
    if not bootstrap_servers:
        raise HTTPException(status_code=400, detail="Bootstrap servers not configured")

    try:
        return kafka_service.produce_message(
            bootstrap_servers, topic_name, body.value, body.key
        )
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/clusters/{id}/connector/{connector_name}/details")
def get_connector_details(id: int, connector_name: str, db: Session = Depends(get_db)):
    """
    Fetch detailed configuration for a specific connector.
    Sensitive values are masked.
    Called on-demand when user clicks on a connector node.
    """
    from lib.kafka import kafka_service
    
    cluster = get_cluster(db, id)
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
def get_consumer_lag(id: int, group_id: str, db: Session = Depends(get_db)):
    """
    Fetch consumer lag per partition for a specific consumer group.
    Called on-demand when user clicks on a consumer node.
    """
    from lib.kafka import kafka_service
    
    cluster = get_cluster(db, id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    bootstrap_servers = cluster.get("bootstrapServers")
    if not bootstrap_servers:
        raise HTTPException(status_code=400, detail="Bootstrap servers not configured")
    
    try:
        return kafka_service.fetch_consumer_lag(bootstrap_servers, group_id)
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
