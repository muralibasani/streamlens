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


# ---------------------------------------------------------------------------
# Cluster registrations (optional: app name + role + topics, no interceptors)
# ---------------------------------------------------------------------------

def get_registrations(db: Session, cluster_id: int):
    try:
        rows = db.execute(
            text("SELECT id, cluster_id, app_name, role, topics, output_topics FROM cluster_registrations WHERE cluster_id = :cid"),
            {"cid": cluster_id},
        ).fetchall()
    except Exception:
        rows = db.execute(
            text("SELECT id, cluster_id, app_name, role, topics FROM cluster_registrations WHERE cluster_id = :cid"),
            {"cid": cluster_id},
        ).fetchall()
    result = []
    for r in rows:
        topics_json = r[4]
        output_topics_json = r[5] if len(r) > 5 else None
        result.append({
            "id": r[0],
            "clusterId": r[1],
            "appName": r[2],
            "role": r[3],
            "topics": _parse_json(topics_json) or [],
            "outputTopics": _parse_json(output_topics_json) if output_topics_json is not None else None,
        })
    return result


def upsert_registration(db: Session, cluster_id: int, app_name: str, role: str, topics: list, output_topics: list | None = None):
    db.execute(text("DELETE FROM cluster_registrations WHERE cluster_id = :cid AND app_name = :app_name"), {"cid": cluster_id, "app_name": app_name})
    # Support DBs that don't have output_topics column yet (add column in migration or create_all)
    try:
        db.execute(
            text("INSERT INTO cluster_registrations (cluster_id, app_name, role, topics, output_topics) VALUES (:cid, :app_name, :role, :topics, :output_topics)"),
            {
                "cid": cluster_id,
                "app_name": app_name,
                "role": role,
                "topics": json.dumps(topics),
                "output_topics": json.dumps(output_topics) if output_topics else None,
            },
        )
    except Exception:
        db.execute(
            text("INSERT INTO cluster_registrations (cluster_id, app_name, role, topics) VALUES (:cid, :app_name, :role, :topics)"),
            {"cid": cluster_id, "app_name": app_name, "role": role, "topics": json.dumps(topics)},
        )
    db.commit()


def delete_registration(db: Session, cluster_id: int, app_name: str):
    db.execute(text("DELETE FROM cluster_registrations WHERE cluster_id = :cid AND app_name = :app_name"), {"cid": cluster_id, "app_name": app_name})
    db.commit()
