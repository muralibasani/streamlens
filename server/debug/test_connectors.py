#!/usr/bin/env python3
"""
Test script to verify Kafka Connect connector discovery.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx

print("=== Testing Kafka Connect Connector Discovery ===\n")

connect_url = "http://localhost:8083"

try:
    with httpx.Client(timeout=10.0) as client:
        # Step 1: List all connectors
        print(f"1. Fetching connectors from: {connect_url}")
        print("-" * 60)
        
        r = client.get(f"{connect_url}/connectors")
        r.raise_for_status()
        connector_names = r.json()
        
        print(f"Found {len(connector_names)} connector(s): {connector_names}\n")
        
        if not connector_names:
            print("⚠️  No connectors found!")
            print("\nTo create a test connector:")
            print("""
curl -X POST http://localhost:8083/connectors \\
  -H "Content-Type: application/json" \\
  -d '{
    "name": "local-file-source",
    "config": {
      "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
      "file": "test.txt",
      "tasks.max": "1",
      "topic": "connect-test",
      "value.converter": "org.apache.kafka.connect.storage.StringConverter",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter"
    }
  }'
""")
            sys.exit(0)
        
        # Step 2: Get details for each connector
        print("\n2. Fetching connector details:")
        print("=" * 60)
        
        for name in connector_names:
            print(f"\nConnector: {name}")
            print("-" * 60)
            
            r2 = client.get(f"{connect_url}/connectors/{name}")
            r2.raise_for_status()
            info = r2.json()
            
            config = info.get("config", {})
            connector_class = config.get("connector.class", "N/A")
            connector_type = info.get("type", "unknown")
            
            # Extract topics
            topics = []
            for key in ["topic", "topics", "topics.regex", "topic.regex"]:
                if key in config:
                    value = config[key]
                    if isinstance(value, str):
                        topics.extend([t.strip() for t in value.split(",") if t.strip()])
                    break
            
            print(f"  Type: {connector_type}")
            print(f"  Class: {connector_class}")
            print(f"  Topics: {topics if topics else 'N/A'}")
            print(f"  Tasks: {len(info.get('tasks', []))}")
            
            # Show what will be created in topology
            print(f"\n  Will create in topology:")
            if topics:
                for topic in topics:
                    if connector_type == "sink":
                        print(f"    topic:{topic} --[consumes]--> connect:{name}")
                    else:
                        print(f"    connect:{name} --[produces]--> topic:{topic}")
            else:
                print(f"    connect:{name} (no topic configured)")
        
        print("\n" + "=" * 60)
        print("\n✅ Connector discovery working correctly!")
        print("\nHow it works in StreamLens:")
        print("  1. Add Connect URL to cluster (http://localhost:8083)")
        print("  2. Click 'Sync' to refresh topology")
        print("  3. Connector nodes appear connected to their topics")
        print("  4. Source connectors: connector → topic")
        print("  5. Sink connectors: topic → connector")
        
except httpx.HTTPError as e:
    print(f"\n❌ Failed to connect to Kafka Connect: {e}")
    print(f"\nMake sure Kafka Connect is running at: {connect_url}")
    print("\nTo start Kafka Connect:")
    print("  - Confluent Platform: confluent local services connect start")
    print("  - Standalone: ./bin/connect-standalone.sh config/connect-standalone.properties")
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
