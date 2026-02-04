#!/usr/bin/env python3
"""
Quick test to verify JMX connection and producer detection.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.kafka import kafka_service

print("=== Testing JMX Producer Detection ===\n")

# Test cluster config
cluster = {
    "bootstrapServers": "localhost:9092",
    "jmxHost": "localhost",
    "jmxPort": 9999,
}

print(f"Testing JMX at {cluster['jmxHost']}:{cluster['jmxPort']}")
print("-" * 50)

try:
    # Try to fetch JMX producers
    producers = kafka_service._fetch_jmx_producers(
        cluster["jmxHost"], 
        cluster["jmxPort"]
    )
    
    if producers:
        print(f"\n✅ SUCCESS! Found {len(producers)} active producer(s):\n")
        for p in producers:
            print(f"  - {p['id']}")
            print(f"    Topics: {', '.join(p['producesTo'])}")
            print(f"    Source: {p['source']}")
            print()
    else:
        print("\n⚠️  JMX connection OK but no active producers detected.")
        print("\nPossible reasons:")
        print("  1. No producers are currently sending messages")
        print("  2. Producers stopped recently (JMX only shows active)")
        print("  3. Messages aren't flowing (check with kafka-consumer-perf-test)")
        
except ImportError as e:
    print(f"\n❌ Missing dependency: {e}")
    print("   Install with: pip install jmxquery")
    
except Exception as e:
    print(f"\n❌ JMX connection failed: {e}")
    print("\nTroubleshooting:")
    print("  1. Is Kafka running with JMX_PORT=9999?")
    print("     Check: ps aux | grep JMX_PORT")
    print()
    print("  2. Restart Kafka with JMX enabled:")
    print("     export JMX_PORT=9999")
    print("     kafka-server-start.sh config/server.properties")
    print()
    print("  3. Test JMX port is open:")
    print("     nc -zv localhost 9999")
    print()

print("\n" + "=" * 50)
print("Next steps:")
print("1. Make sure producers are actively sending messages")
print("2. Run: ./resources/create_data.sh")
print("3. Restart backend: uvicorn main:app --reload")
print("4. Click 'Sync' in UI")
