#!/usr/bin/env python3
"""
Quick script to configure JMX for your cluster.
"""
import sys
from db import SessionLocal
from storage import get_clusters
from sqlalchemy import text

def configure_jmx(cluster_id: int, jmx_host: str, jmx_port: int):
    """Configure JMX for a cluster."""
    db = SessionLocal()
    try:
        # Update the cluster
        db.execute(
            text("UPDATE clusters SET jmx_host = :host, jmx_port = :port WHERE id = :id"),
            {"host": jmx_host, "port": jmx_port, "id": cluster_id}
        )
        db.commit()
        print(f"✅ Configured cluster {cluster_id} with JMX: {jmx_host}:{jmx_port}")
        
        # Verify
        result = db.execute(
            text("SELECT id, name, jmx_host, jmx_port FROM clusters WHERE id = :id"),
            {"id": cluster_id}
        ).fetchone()
        
        if result:
            print(f"\nCluster details:")
            print(f"  ID: {result[0]}")
            print(f"  Name: {result[1]}")
            print(f"  JMX Host: {result[2]}")
            print(f"  JMX Port: {result[3]}")
        
    finally:
        db.close()

def list_clusters():
    """List all clusters."""
    db = SessionLocal()
    try:
        clusters = get_clusters(db)
        print("Available clusters:")
        for c in clusters:
            jmx_status = f"JMX: {c.get('jmxHost')}:{c.get('jmxPort')}" if c.get('jmxHost') else "JMX: Not configured"
            print(f"  [{c['id']}] {c['name']} - {jmx_status}")
    finally:
        db.close()

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
    print("1. Restart your backend server")
    print("2. Start a producer and send messages")
    print("3. Click 'Sync' in the UI")
    print("4. Look for ⚡ JMX producers!")
