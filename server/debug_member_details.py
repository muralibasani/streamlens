#!/usr/bin/env python3
"""Debug what's actually in the consumer group member objects."""

from confluent_kafka.admin import AdminClient

bootstrap = "localhost:9092"
admin = AdminClient({"bootstrap.servers": bootstrap})

print("=== Detailed Member Inspection ===\n")

groups = admin.list_groups(timeout=10)

for group in groups:
    if getattr(group, 'protocol_type', '') == 'consumer':
        print(f"Group: {group.id}")
        print(f"  Protocol type: {group.protocol_type}")
        print(f"  Protocol: {getattr(group, 'protocol', 'N/A')}")
        print(f"  State: {getattr(group, 'state', 'N/A')}")
        
        if hasattr(group, 'members'):
            print(f"  Members: {len(group.members)}")
            
            for i, member in enumerate(group.members, 1):
                print(f"\n  Member {i}:")
                print(f"    Type: {type(member)}")
                print(f"    Attributes: {dir(member)}")
                print(f"    Client ID: {getattr(member, 'client_id', 'N/A')}")
                print(f"    Host: {getattr(member, 'host', 'N/A')}")
                print(f"    Member ID: {getattr(member, 'id', 'N/A')}")
                
                # Check all possible metadata/assignment attributes
                for attr in ['member_metadata', 'metadata', 'member_assignment', 'assignment']:
                    if hasattr(member, attr):
                        val = getattr(member, attr)
                        print(f"    {attr}: {type(val)} - {val[:100] if val else 'None'}")
                    else:
                        print(f"    {attr}: NOT PRESENT")
        print()
