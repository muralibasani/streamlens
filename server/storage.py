"""
Storage layer. All SQL is dialect-agnostic (SQLite and PostgreSQL).
"""
import json
from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session

from db import SnapshotData


def _parse_json(value):
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def get_clusters(db: Session):
    rows = db.execute(text(
        "SELECT id, name, bootstrap_servers, schema_registry_url, connect_url, jmx_host, jmx_port, created_at FROM clusters ORDER BY id"
    )).fetchall()
    return [
        {
            "id": r[0],
            "name": r[1],
            "bootstrapServers": r[2],
            "schemaRegistryUrl": r[3],
            "connectUrl": r[4],
            "jmxHost": r[5],
            "jmxPort": r[6],
            "createdAt": r[7].isoformat() if hasattr(r[7], "isoformat") else r[7],
        }
        for r in rows
    ]


def get_cluster(db: Session, id: int):
    row = db.execute(
        text("SELECT id, name, bootstrap_servers, schema_registry_url, connect_url, jmx_host, jmx_port, created_at FROM clusters WHERE id = :id"),
        {"id": id},
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "name": row[1],
        "bootstrapServers": row[2],
        "schemaRegistryUrl": row[3],
        "connectUrl": row[4],
        "jmxHost": row[5],
        "jmxPort": row[6],
        "createdAt": row[7].isoformat() if hasattr(row[7], "isoformat") else row[7],
    }


def create_cluster(db: Session, name: str, bootstrap_servers: str, schema_registry_url: str | None = None, connect_url: str | None = None, jmx_host: str | None = None, jmx_port: int | None = None):
    result = db.execute(
        text("""
            INSERT INTO clusters (name, bootstrap_servers, schema_registry_url, connect_url, jmx_host, jmx_port)
            VALUES (:name, :bootstrap_servers, :schema_registry_url, :connect_url, :jmx_host, :jmx_port)
            RETURNING id, name, bootstrap_servers, schema_registry_url, connect_url, jmx_host, jmx_port, created_at
        """),
        {
            "name": name,
            "bootstrap_servers": bootstrap_servers,
            "schema_registry_url": schema_registry_url,
            "connect_url": connect_url,
            "jmx_host": jmx_host,
            "jmx_port": jmx_port,
        },
    )
    row = result.fetchone()
    db.commit()
    return {
        "id": row[0],
        "name": row[1],
        "bootstrapServers": row[2],
        "schemaRegistryUrl": row[3],
        "connectUrl": row[4],
        "jmxHost": row[5],
        "jmxPort": row[6],
        "createdAt": row[7].isoformat() if hasattr(row[7], "isoformat") else row[7],
    }


def update_cluster(
    db: Session,
    id: int,
    name: str,
    bootstrap_servers: str,
    schema_registry_url: str | None = None,
    connect_url: str | None = None,
    jmx_host: str | None = None,
    jmx_port: int | None = None,
):
    row = db.execute(
        text("""
            UPDATE clusters 
            SET name = :name,
                bootstrap_servers = :bs,
                schema_registry_url = :sr,
                connect_url = :cu,
                jmx_host = :jmx_host,
                jmx_port = :jmx_port
            WHERE id = :id
            RETURNING id, name, bootstrap_servers, schema_registry_url, connect_url, created_at, jmx_host, jmx_port
        """),
        {
            "id": id,
            "name": name,
            "bs": bootstrap_servers,
            "sr": schema_registry_url,
            "cu": connect_url,
            "jmx_host": jmx_host,
            "jmx_port": jmx_port,
        },
    ).fetchone()
    if not row:
        return None
    db.commit()
    return {
        "id": row[0],
        "name": row[1],
        "bootstrapServers": row[2],
        "schemaRegistryUrl": row[3],
        "connectUrl": row[4],
        "createdAt": row[5].isoformat() if hasattr(row[5], "isoformat") else row[5],
        "jmxHost": row[6],
        "jmxPort": row[7],
    }

def delete_cluster(db: Session, id: int):
    db.execute(text("DELETE FROM clusters WHERE id = :id"), {"id": id})
    db.commit()


def get_latest_snapshot(db: Session, cluster_id: int):
    row = db.execute(
        text("SELECT id, cluster_id, data, created_at FROM snapshots WHERE cluster_id = :cid ORDER BY created_at DESC LIMIT 1"),
        {"cid": cluster_id},
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "clusterId": row[1],
        "data": _parse_json(row[2]),
        "createdAt": row[3].isoformat() if hasattr(row[3], "isoformat") else row[3],
    }


def create_snapshot(db: Session, cluster_id: int, data: dict):
    # Use bindparam with SnapshotData so SQLite (TEXT) and PostgreSQL (JSONB) both work
    stmt = text(
        "INSERT INTO snapshots (cluster_id, data) VALUES (:cid, :data) RETURNING id, cluster_id, data, created_at"
    ).bindparams(bindparam("data", type_=SnapshotData))
    result = db.execute(stmt, {"cid": cluster_id, "data": data})
    row = result.fetchone()
    db.commit()
    return {
        "id": row[0],
        "clusterId": row[1],
        "data": _parse_json(row[2]),
        "createdAt": row[3].isoformat() if hasattr(row[3], "isoformat") else row[3],
    }


