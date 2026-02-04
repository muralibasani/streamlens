#!/usr/bin/env python3
"""
Comprehensive Kafka connection and consumer group debugging.
"""
import sys
from confluent_kafka.admin import AdminClient

def debug_kafka(bootstrap_servers="localhost:9092"):
    print(f"🔍 Debugging Kafka connection to {bootstrap_servers}\n")
    
    # Test 1: Can we connect at all?
    print("=== Test 1: Basic Connection ===")
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin.list_topics(timeout=10)
        print(f"✓ Successfully connected to Kafka")
        print(f"✓ Cluster has {len(metadata.topics)} topics")
    except Exception as e:
        print(f"❌ Cannot connect to Kafka: {e}")
        print("\nTroubleshooting:")
        print("1. Is Kafka running? Check with: ps aux | grep kafka")
        print("2. Is the bootstrap server correct? Try: netstat -an | grep 9092")
        return
    
    # Test 2: Does the topic exist?
    print("\n=== Test 2: Topic Existence ===")
    topics = list(metadata.topics.keys())
    print(f"Found {len(topics)} topic(s):")
    for topic in sorted(topics):
        if not topic.startswith("__"):
            partitions = len(metadata.topics[topic].partitions)
            print(f"  ✓ {topic} ({partitions} partition(s))")
    
    if "testtopic" not in topics:
        print("\n⚠️  Topic 'testtopic' does NOT exist!")
        print("\nCreate it with:")
        print("  ./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic testtopic --partitions 1 --replication-factor 1")
    else:
        print("\n✓ Topic 'testtopic' exists")
    
    # Test 3: Consumer groups
    print("\n=== Test 3: Consumer Groups ===")
    try:
        if hasattr(admin, "list_consumer_groups"):
            result = admin.list_consumer_groups(request_timeout=10)
            groups = result.valid if hasattr(result, "valid") else []
            group_ids = [g.group_id for g in groups]
        else:
            result = admin.list_groups(timeout=10)
            group_ids = [g.id for g in result] if result else []
        
        if not group_ids:
            print("❌ No consumer groups found")
            print("\nThis means your console consumer is NOT registered with Kafka.")
            print("\nPossible reasons:")
            print("1. The console consumer hasn't started properly")
            print("2. The console consumer is connecting to a different broker")
            print("3. The topic doesn't exist (so consumer can't subscribe)")
            print("4. The consumer exited with an error")
            print("\n📝 Please share the FULL output from your console consumer terminal")
        else:
            print(f"✓ Found {len(group_ids)} consumer group(s):")
            for gid in group_ids:
                print(f"  - {gid}")
    except Exception as e:
        print(f"❌ Error listing groups: {e}")
    
    # Test 4: Try to describe all groups (including internal)
    print("\n=== Test 4: All Groups (including internal) ===")
    try:
        result = admin.list_groups(timeout=10)
        all_groups = [g.id for g in result] if result else []
        print(f"Total groups: {len(all_groups)}")
        for gid in all_groups:
            print(f"  - {gid}")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n" + "="*60)
    print("NEXT STEPS:")
    print("="*60)
    print("\n1. Verify your console consumer is running:")
    print("   - Check the terminal where you ran it")
    print("   - Look for any error messages")
    print("   - It should show 'Subscribed to topic(s): testtopic' or similar")
    print("\n2. If the consumer exited, try running it again:")
    print("   ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic testtopic --group mygroup")
    print("\n3. Send a test message to trigger assignment:")
    print("   echo 'test' | ./bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic testtopic")
    print("\n4. Share the console consumer output here so we can debug further")

if __name__ == "__main__":
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
    debug_kafka(bootstrap)
