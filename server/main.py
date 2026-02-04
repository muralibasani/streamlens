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

# Load environment variables from .env.dev or .env file
load_dotenv(".env.dev")  # Load .env.dev first (dev environment)
load_dotenv()  # Then load .env if it exists (will not override existing vars)

from db import get_db, init_db
from storage import (
    get_clusters,
    get_cluster,
    create_cluster as db_create_cluster,
    delete_cluster as db_delete_cluster,
    get_latest_snapshot,
    create_snapshot,
    get_registrations,
    upsert_registration,
    delete_registration,
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


class RegistrationBody(BaseModel):
    appName: str
    role: str  # "producer", "consumer", or "streams"
    topics: list[str]  # input topics (consumesFrom for consumer/streams, producesTo for producer)
    outputTopics: list[str] | None = None  # for streams: output topics (producesTo)


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
                    regs = get_registrations(db, c["id"])
                    graph = build_topology(c["id"], c, regs)
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


@app.get("/api/clusters/{id}")
def clusters_get(id: int, db: Session = Depends(get_db)):
    cluster = get_cluster(db, id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return cluster


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
        regs = get_registrations(db, cluster["id"])
        graph = build_topology(cluster["id"], cluster, regs)
        create_snapshot(db, cluster["id"], graph)
        return cluster
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail={"message": str(e), "field": None})
    except Exception as e:
        raise HTTPException(status_code=400, detail={"message": str(e), "field": None})


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
        regs = get_registrations(db, id)
        graph = build_topology(id, cluster, regs)
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
        regs = get_registrations(db, id)
        graph = build_topology(id, cluster, regs)
        return create_snapshot(db, id, graph)
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/api/clusters/{id}/registrations")
def registrations_list(id: int, db: Session = Depends(get_db)):
    if not get_cluster(db, id):
        raise HTTPException(status_code=404, detail="Cluster not found")
    return get_registrations(db, id)


@app.post("/api/clusters/{id}/registrations")
def registration_upsert(id: int, body: RegistrationBody, db: Session = Depends(get_db)):
    if not get_cluster(db, id):
        raise HTTPException(status_code=404, detail="Cluster not found")
    if body.role not in ("producer", "consumer", "streams"):
        raise HTTPException(status_code=400, detail="role must be 'producer', 'consumer', or 'streams'")
    output_topics = body.outputTopics if body.role == "streams" else None
    upsert_registration(db, id, body.appName.strip(), body.role, body.topics, output_topics=output_topics)
    out = {"appName": body.appName, "role": body.role, "topics": body.topics}
    if body.outputTopics is not None:
        out["outputTopics"] = body.outputTopics
    return out


@app.delete("/api/clusters/{id}/registrations/{app_name}")
def registration_delete(id: int, app_name: str, db: Session = Depends(get_db)):
    if not get_cluster(db, id):
        raise HTTPException(status_code=404, detail="Cluster not found")
    delete_registration(db, id, app_name)
    return Response(status_code=204)


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
