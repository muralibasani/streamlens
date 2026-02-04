#!/usr/bin/env python3
"""
Check if we can detect active producers via Kafka APIs.
"""

from confluent_kafka.admin import AdminClient
from confluent_kafka import Producer
import time

bootstrap = "localhost:9092"

print("=== Checking Producer Detection Options ===\n")

admin = AdminClient({"bootstrap.servers": bootstrap})

# Check 1: Can we see active clients?
print("1. Checking cluster metadata for client info...")
try:
    metadata = admin.list_topics(timeout=10)
    print(f"   Cluster ID: {metadata.cluster_id}")
    print(f"   Controller: {metadata.controller_id}")
    print(f"   Brokers: {len(metadata.brokers)}")
    
    # Look for any client information
    for broker_id, broker in metadata.brokers.items():
        print(f"   Broker {broker_id}: {broker.host}:{broker.port}")
    
    # Check if metadata has client info (it usually doesn't)
    print(f"   Metadata attributes: {[attr for attr in dir(metadata) if not attr.startswith('_')]}")
except Exception as e:
    print(f"   Error: {e}")

# Check 2: Create a test producer and see if we can detect it
print("\n2. Creating a test producer...")
try:
    test_producer = Producer({
        'bootstrap.servers': bootstrap,
        'client.id': 'test-producer-discovery',
    })
    
    # Send a test message
    test_producer.produce('testtopic', value=b'detection test')
    test_producer.flush()
    
    print("   Test producer created and sent message")
    
    # Now check if we can detect it
    print("\n3. Checking if we can detect the test producer...")
    
    # Try list_groups to see if producers show up (they won't, but let's confirm)
    groups = admin.list_groups(timeout=10)
    producer_count = 0
    for group in groups:
        if getattr(group, 'protocol_type', '') not in ['consumer', '']:
            print(f"   Found non-consumer group: {group.id} (type: {group.protocol_type})")
            producer_count += 1
    
    if producer_count == 0:
        print("   ❌ No producers detected via list_groups()")
    
except Exception as e:
    print(f"   Error: {e}")

print("\n4. Checking for transactional producers...")
try:
    # Note: AdminClient doesn't have list_transactions() in confluent-kafka-python
    # This would require running kafka-transactions.sh CLI
    print("   AdminClient doesn't expose transaction listing API")
    print("   Would need to use: kafka-transactions.sh --bootstrap-server localhost:9092 list")
except Exception as e:
    print(f"   Error: {e}")

print("\n" + "="*60)
print("CONCLUSION:")
print("="*60)
print("""
Unfortunately, Kafka's AdminClient API doesn't provide:
- List of active producers
- Mapping of producer ID or client ID to topics
- Real-time producer connections

Why?
- Producers are stateless from Kafka's perspective
- Broker only tracks them for idempotency/transactions
- No "producer group" concept like consumer groups

Possible solutions:
1. ✅ ACLs (already implemented) - shows who CAN produce
2. ❌ JMX metrics - requires broker JMX access (complex)
3. ❌ Broker logs - not practical for real-time
4. ✅ Manual registration (current approach) - simplest

Recommendation: Keep manual registration for producers
""")
