"""
Storage layer: clusters in a JSON file, topology snapshots in memory.
Admin can edit data/clusters.json and restart the server; no database or login required.
"""
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

_SERVER_DIR = Path(__file__).resolve().parent
_CLUSTERS_PATH = Path(os.environ.get("CLUSTERS_JSON", _SERVER_DIR / "data" / "clusters.json"))
_lock = threading.Lock()
_snapshot_cache: dict[int, dict] = {}


def _ensure_clusters_file():
    _CLUSTERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not _CLUSTERS_PATH.exists():
        with _lock:
            _CLUSTERS_PATH.write_text(json.dumps({"clusters": []}, indent=2), encoding="utf-8")


def _read_raw() -> dict:
    """Read full clusters.json (clusters + root-level config)."""
    _ensure_clusters_file()
    with _lock:
        raw = json.loads(_CLUSTERS_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {"clusters": []}
    return dict(raw)


def _read_clusters() -> list[dict]:
    raw = _read_raw()
    clusters = raw.get("clusters")
    if not isinstance(clusters, list):
        return []
    return list(clusters)


def _write_clusters(clusters: list[dict]) -> None:
    _ensure_clusters_file()
    raw = _read_raw()
    raw["clusters"] = clusters
    raw.pop("enableKafkaEventProduceFromUi", None)  # now per-cluster only
    with _lock:
        _CLUSTERS_PATH.write_text(
            json.dumps(raw, indent=2),
            encoding="utf-8",
        )


def _bool_from(val) -> bool:
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("true", "1", "yes")


def _cluster_from_row(c: dict) -> dict:
    """Normalize to API shape (camelCase, createdAt). Per-cluster enableKafkaEventProduceFromUi and optional SSL."""
    out = {
        "id": c["id"],
        "name": c["name"],
        "bootstrapServers": c.get("bootstrapServers") or c.get("bootstrap_servers") or "",
        "schemaRegistryUrl": c.get("schemaRegistryUrl") or c.get("schema_registry_url"),
        "connectUrl": c.get("connectUrl") or c.get("connect_url"),
        "jmxHost": c.get("jmxHost") or c.get("jmx_host"),
        "jmxPort": c.get("jmxPort") or c.get("jmx_port"),
        "createdAt": c.get("createdAt") or c.get("created_at") or "",
        "enableKafkaEventProduceFromUi": _bool_from(
            c.get("enableKafkaEventProduceFromUi") or c.get("enable_kafka_event_produce_from_ui")
        ),
    }
    # Optional SSL / security (for connecting on port 9093 SSL, etc.)
    if c.get("securityProtocol") or c.get("security_protocol"):
        out["securityProtocol"] = c.get("securityProtocol") or c.get("security_protocol")
    if c.get("sslCaLocation") or c.get("ssl_ca_location") or c.get("ssl.ca.location"):
        out["sslCaLocation"] = c.get("sslCaLocation") or c.get("ssl_ca_location") or c.get("ssl.ca.location")
    if c.get("sslCertificateLocation") or c.get("ssl_certificate_location"):
        out["sslCertificateLocation"] = c.get("sslCertificateLocation") or c.get("ssl_certificate_location")
    if c.get("sslKeyLocation") or c.get("ssl_key_location"):
        out["sslKeyLocation"] = c.get("sslKeyLocation") or c.get("ssl_key_location")
    if c.get("sslKeyPassword") or c.get("ssl_key_password"):
        out["sslKeyPassword"] = c.get("sslKeyPassword") or c.get("ssl_key_password")
    if c.get("sslTruststoreLocation") or c.get("ssl_truststore_location"):
        out["sslTruststoreLocation"] = c.get("sslTruststoreLocation") or c.get("ssl_truststore_location")
    if c.get("sslTruststorePassword") or c.get("ssl_truststore_password"):
        out["sslTruststorePassword"] = c.get("sslTruststorePassword") or c.get("ssl_truststore_password")
    if c.get("sslKeystoreLocation") or c.get("ssl_keystore_location"):
        out["sslKeystoreLocation"] = c.get("sslKeystoreLocation") or c.get("ssl_keystore_location")
    if c.get("sslKeystoreType") or c.get("ssl_keystore_type"):
        out["sslKeystoreType"] = c.get("sslKeystoreType") or c.get("ssl_keystore_type")
    if c.get("sslKeystorePassword") or c.get("ssl_keystore_password"):
        out["sslKeystorePassword"] = c.get("sslKeystorePassword") or c.get("ssl_keystore_password")
    if "sslEndpointIdentificationAlgorithm" in c or "ssl_endpoint_identification_algorithm" in c:
        out["sslEndpointIdentificationAlgorithm"] = c.get("sslEndpointIdentificationAlgorithm") or c.get("ssl_endpoint_identification_algorithm") or ""
    if "enableSslCertificateVerification" in c or "enable_ssl_certificate_verification" in c:
        val = c.get("enableSslCertificateVerification") if c.get("enableSslCertificateVerification") is not None else c.get("enable_ssl_certificate_verification")
        out["enableSslCertificateVerification"] = val if val is not None else True
    return out


def get_clusters() -> list[dict]:
    rows = _read_clusters()
    return [_cluster_from_row(c) for c in rows]


def get_cluster(id: int) -> dict | None:
    clusters = _read_clusters()
    for c in clusters:
        if c.get("id") == id:
            return _cluster_from_row(c)
    return None


def create_cluster(
    name: str,
    bootstrap_servers: str,
    schema_registry_url: str | None = None,
    connect_url: str | None = None,
    jmx_host: str | None = None,
    jmx_port: int | None = None,
    enable_kafka_event_produce_from_ui: bool = False,
) -> dict:
    clusters = _read_clusters()
    next_id = max((c.get("id") or 0 for c in clusters), default=0) + 1
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new_cluster = {
        "id": next_id,
        "name": name,
        "bootstrapServers": bootstrap_servers,
        "schemaRegistryUrl": schema_registry_url,
        "connectUrl": connect_url,
        "jmxHost": jmx_host,
        "jmxPort": jmx_port,
        "createdAt": now,
        "enableKafkaEventProduceFromUi": enable_kafka_event_produce_from_ui,
    }
    clusters.append(new_cluster)
    _write_clusters(clusters)
    return _cluster_from_row(new_cluster)


def update_cluster(
    id: int,
    name: str,
    bootstrap_servers: str,
    schema_registry_url: str | None = None,
    connect_url: str | None = None,
    jmx_host: str | None = None,
    jmx_port: int | None = None,
    enable_kafka_event_produce_from_ui: bool | None = None,
) -> dict | None:
    clusters = _read_clusters()
    for i, c in enumerate(clusters):
        if c.get("id") == id:
            existing = clusters[i]
            if enable_kafka_event_produce_from_ui is None:
                enable_kafka_event_produce_from_ui = _bool_from(
                    existing.get("enableKafkaEventProduceFromUi") or existing.get("enable_kafka_event_produce_from_ui")
                )
            clusters[i] = {
                "id": id,
                "name": name,
                "bootstrapServers": bootstrap_servers,
                "schemaRegistryUrl": schema_registry_url,
                "connectUrl": connect_url,
                "jmxHost": jmx_host,
                "jmxPort": jmx_port,
                "createdAt": existing.get("createdAt") or existing.get("created_at") or "",
                "enableKafkaEventProduceFromUi": enable_kafka_event_produce_from_ui,
                "securityProtocol": existing.get("securityProtocol") or existing.get("security_protocol"),
                "sslCaLocation": existing.get("sslCaLocation") or existing.get("ssl_ca_location"),
                "sslCertificateLocation": existing.get("sslCertificateLocation") or existing.get("ssl_certificate_location"),
                "sslKeyLocation": existing.get("sslKeyLocation") or existing.get("ssl_key_location"),
                "sslKeyPassword": existing.get("sslKeyPassword") or existing.get("ssl_key_password"),
                "sslTruststoreLocation": existing.get("sslTruststoreLocation") or existing.get("ssl_truststore_location"),
                "sslTruststorePassword": existing.get("sslTruststorePassword") or existing.get("ssl_truststore_password"),
                "sslKeystoreLocation": existing.get("sslKeystoreLocation") or existing.get("ssl_keystore_location"),
                "sslKeystoreType": existing.get("sslKeystoreType") or existing.get("ssl_keystore_type"),
                "sslKeystorePassword": existing.get("sslKeystorePassword") or existing.get("ssl_keystore_password"),
                "sslEndpointIdentificationAlgorithm": existing.get("sslEndpointIdentificationAlgorithm") or existing.get("ssl_endpoint_identification_algorithm"),
                "enableSslCertificateVerification": existing.get("enableSslCertificateVerification") if existing.get("enableSslCertificateVerification") is not None else existing.get("enable_ssl_certificate_verification"),
            }
            _write_clusters(clusters)
            return _cluster_from_row(clusters[i])
    return None


def delete_cluster(id: int) -> None:
    clusters = _read_clusters()
    clusters = [c for c in clusters if c.get("id") != id]
    _write_clusters(clusters)
    with _lock:
        _snapshot_cache.pop(id, None)


def get_latest_snapshot(cluster_id: int) -> dict | None:
    with _lock:
        return _snapshot_cache.get(cluster_id)


def create_snapshot(cluster_id: int, data: dict) -> dict:
    snapshot = {
        "id": cluster_id,
        "clusterId": cluster_id,
        "data": data,
        "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with _lock:
        _snapshot_cache[cluster_id] = snapshot
    return snapshot
