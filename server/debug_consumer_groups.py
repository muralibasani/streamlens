#!/usr/bin/env python3
"""
Debug script to check if consumer groups are visible to Kafka AdminClient.
Run this to verify your console consumer is showing up.
"""
import sys
from confluent_kafka.admin import AdminClient

def check_consumer_groups(bootstrap_servers="localhost:9092"):
    print(f"Connecting to Kafka at {bootstrap_servers}...")
    
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    
    print("\n=== Listing Consumer Groups ===")
    try:
        if hasattr(admin, "list_consumer_groups"):
            result = admin.list_consumer_groups(request_timeout=10)
            groups = result.valid if hasattr(result, "valid") else []
            group_ids = [g.group_id for g in groups]
        else:
            result = admin.list_groups(timeout=10)
            group_ids = [g.id for g in result] if result else []
        
        if not group_ids:
            print("❌ No consumer groups found!")
            print("\nTroubleshooting:")
            print("1. Make sure your console consumer is running")
            print("2. The consumer needs to be in 'Stable' state (may take a few seconds)")
            print("3. Try sending a message to the topic to trigger partition assignment")
            return
        
        print(f"✓ Found {len(group_ids)} consumer group(s):")
        for gid in group_ids:
            print(f"  - {gid}")
        
        print("\n=== Describing Consumer Groups ===")
        if hasattr(admin, "describe_consumer_groups"):
            described = admin.describe_consumer_groups(group_ids, request_timeout=10)
        else:
            described = admin.describe_groups(group_ids, request_timeout=10)
        
        for gid, future in described.items():
            try:
                group_info = future.result()
                state = getattr(group_info, 'state', 'unknown')
                members = group_info.members if hasattr(group_info, 'members') else []
                
                print(f"\nGroup: {gid}")
                print(f"  State: {state}")
                print(f"  Members: {len(members)}")
                
                if not members:
                    print("  ⚠️  No members in this group (may be empty or rebalancing)")
                
                for i, member in enumerate(members, 1):
                    print(f"  Member {i}:")
                    print(f"    Client ID: {getattr(member, 'client_id', 'N/A')}")
                    print(f"    Host: {getattr(member, 'host', 'N/A')}")
                    
                    if hasattr(member, "assignment") and member.assignment:
                        tps = getattr(member.assignment, "topic_partitions", None) or []
                        if tps:
                            topics = set()
                            for tp in tps:
                                topic = getattr(tp, "topic", str(tp))
                                partition = getattr(tp, "partition", "?")
                                topics.add(topic)
                                print(f"    Assigned: {topic}[{partition}]")
                            print(f"    ✓ Topics: {', '.join(sorted(topics))}")
                        else:
                            print(f"    ⚠️  No partition assignments yet")
                    else:
                        print(f"    ⚠️  No assignment information")
                        
            except Exception as e:
                print(f"  ❌ Error describing group {gid}: {e}")
        
        print("\n=== Summary ===")
        print("If you see '⚠️ No partition assignments yet', try one of these:")
        print("1. Send a message to the topic: echo 'test' | kafka-console-producer --bootstrap-server localhost:9092 --topic testtopic")
        print("2. Wait a few seconds for the consumer to stabilize")
        print("3. Check that the topic exists and has partitions")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
    check_consumer_groups(bootstrap)
