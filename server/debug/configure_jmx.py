#!/usr/bin/env python3
"""
Quick script to configure JMX for your cluster.
Uses the same JSON cluster storage as the server (server/data/clusters.json).
"""
import sys
from pathlib import Path

# Add parent directory to path so we can import from server modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from storage import get_clusters, get_cluster, update_cluster


def configure_jmx(cluster_id: int, jmx_host: str, jmx_port: int):
    """Configure JMX for a cluster."""
    existing = get_cluster(cluster_id)
    if not existing:
        print(f"❌ Cluster {cluster_id} not found.")
        return
    updated = update_cluster(
        id=cluster_id,
        name=existing["name"],
        bootstrap_servers=existing["bootstrapServers"],
        schema_registry_url=existing.get("schemaRegistryUrl"),
        connect_url=existing.get("connectUrl"),
        jmx_host=jmx_host,
        jmx_port=jmx_port,
    )
    if updated:
        print(f"✅ Configured cluster {cluster_id} with JMX: {jmx_host}:{jmx_port}")
        print(f"\nCluster details:")
        print(f"  ID: {updated['id']}")
        print(f"  Name: {updated['name']}")
        print(f"  JMX Host: {updated.get('jmxHost')}")
        print(f"  JMX Port: {updated.get('jmxPort')}")
    else:
        print(f"❌ Failed to update cluster {cluster_id}")


def list_clusters():
    """List all clusters."""
    clusters = get_clusters()
    print("Available clusters:")
    for c in clusters:
        jmx_status = f"JMX: {c.get('jmxHost')}:{c.get('jmxPort')}" if c.get('jmxHost') else "JMX: Not configured"
        print(f"  [{c['id']}] {c['name']} - {jmx_status}")


if __name__ == "__main__":
    print("=== JMX Configuration Tool ===\n")

    # List clusters first
    list_clusters()

    # Get input
    if len(sys.argv) >= 4:
        cluster_id = int(sys.argv[1])
        jmx_host = sys.argv[2]
        jmx_port = int(sys.argv[3])
    else:
        print("\nUsage: python configure_jmx.py <cluster_id> <jmx_host> <jmx_port>")
        print("Example: python configure_jmx.py 1 localhost 9999")
        print("\nOr run interactively:")
        print("⚠️  Use the ID number in brackets [1], NOT the Kafka cluster UUID")

        cluster_id_str = input("\nEnter cluster ID (the number in brackets, e.g., 1): ").strip()
        try:
            cluster_id = int(cluster_id_str)
        except ValueError:
            print(f"❌ Invalid cluster ID: '{cluster_id_str}'")
            print("   Please use the NUMBER in brackets (e.g., 1)")
            sys.exit(1)

        jmx_host = input("Enter JMX host (e.g., localhost): ").strip()
        jmx_port = int(input("Enter JMX port (e.g., 9999): "))

    print()
    configure_jmx(cluster_id, jmx_host, jmx_port)

    print("\n✅ Done! Now:")
    print("1. Restart your backend server (if you prefer; or changes are already on disk)")
    print("2. Start a producer and send messages")
    print("3. Click 'Sync' in the UI")
    print("4. Look for ⚡ JMX producers!")
