#!/usr/bin/env python3
"""Quick test of the new consumer group detection."""

from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition
import re

bootstrap = "localhost:9092"

print("=== Testing Consumer Group Detection ===\n")

# Step 1: List groups with full metadata
print("1. Listing groups...")
admin = AdminClient({"bootstrap.servers": bootstrap})
groups = admin.list_groups(timeout=10)

consumer_group_metadata = {}
for group in groups:
    if getattr(group, 'protocol_type', '') == 'consumer':
        consumer_group_metadata[group.id] = group
        print(f"   Found: {group.id}")

if not consumer_group_metadata:
    print("   ❌ No consumer groups found!")
    exit(1)

print(f"\n2. Analyzing {len(consumer_group_metadata)} group(s)...\n")

# Get all topics for validation
all_topics = admin.list_topics(timeout=5).topics.keys()

for group_id, group_metadata in consumer_group_metadata.items():
    print(f"Group: {group_id}")
    topics_found = set()
    
    # Method 1: Check member metadata for subscribed topics
    if hasattr(group_metadata, 'members') and group_metadata.members:
        print(f"  Members: {len(group_metadata.members)}")
        
        for i, member in enumerate(group_metadata.members, 1):
            print(f"  Member {i}:")
            print(f"    Client ID: {getattr(member, 'client_id', 'N/A')}")
            
            # Check metadata (subscription)
            if hasattr(member, 'metadata') and member.metadata:
                try:
                    metadata_bytes = member.metadata
                    decoded = metadata_bytes.decode('utf-8', errors='ignore')
                    
                    # Look for known topics in the decoded metadata
                    for topic in all_topics:
                        if not topic.startswith('__') and topic in decoded:
                            topics_found.add(topic)
                            print(f"    ✓ Subscribed to: {topic} (from metadata)")
                            
                except Exception as e:
                    print(f"    Could not parse metadata: {e}")
            
            # Check assignment (assigned partitions)
            if hasattr(member, 'assignment') and member.assignment:
                try:
                    assignment_bytes = member.assignment
                    decoded = assignment_bytes.decode('utf-8', errors='ignore')
                    
                    # Look for known topics in the decoded assignment
                    for topic in all_topics:
                        if not topic.startswith('__') and topic in decoded:
                            if topic not in topics_found:  # Don't duplicate
                                topics_found.add(topic)
                                print(f"    ✓ Assigned to: {topic} (from assignment)")
                            
                except Exception as e:
                    print(f"    Could not parse assignment: {e}")
    else:
        print(f"  No active members")
    
    # Method 2: Check committed offsets
    if not topics_found:
        print(f"  Checking committed offsets...")
        temp_consumer = Consumer({
            'bootstrap.servers': bootstrap,
            'group.id': f'_temp_query_{group_id}',
            'enable.auto.commit': False,
        })
        
        cluster_metadata = temp_consumer.list_topics(timeout=5)
        
        for topic_name in cluster_metadata.topics.keys():
            if topic_name.startswith('__'):
                continue
            
            topic_metadata = cluster_metadata.topics[topic_name]
            partitions = list(topic_metadata.partitions.keys())
            
            if not partitions:
                continue
            
            tps = [TopicPartition(topic_name, p) for p in partitions]
            committed = temp_consumer.committed(tps, timeout=2)
            
            for tp in committed:
                if tp.offset >= 0:
                    topics_found.add(topic_name)
                    print(f"    ✓ {topic_name} (committed offset: {tp.offset})")
                    break
        
        temp_consumer.close()
    
    if not topics_found:
        print(f"  ⚠️  No topics detected")
    
    print()

print("✅ Done!")
