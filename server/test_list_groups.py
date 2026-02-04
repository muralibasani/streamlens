#!/usr/bin/env python3
"""Test different methods of listing consumer groups."""

from confluent_kafka.admin import AdminClient

admin = AdminClient({"bootstrap.servers": "localhost:9092"})

print("=== Method 1: list_groups() ===")
try:
    result = admin.list_groups(timeout=10)
    print(f"Type: {type(result)}")
    print(f"Result: {result}")
    
    if result:
        for group in result:
            print(f"\nGroup:")
            print(f"  ID: {group.id if hasattr(group, 'id') else group}")
            print(f"  Protocol: {group.protocol_type if hasattr(group, 'protocol_type') else 'N/A'}")
            print(f"  Protocol name: {group.protocol if hasattr(group, 'protocol') else 'N/A'}")
            print(f"  Members: {len(group.members) if hasattr(group, 'members') else 'N/A'}")
            
            if hasattr(group, 'members'):
                for member in group.members:
                    print(f"    Member ID: {member.id if hasattr(member, 'id') else member}")
                    if hasattr(member, 'assignment'):
                        print(f"      Assignment: {member.assignment}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("=== Method 2: list_consumer_groups() ===")
try:
    if hasattr(admin, 'list_consumer_groups'):
        result = admin.list_consumer_groups(request_timeout=10)
        print(f"Type: {type(result)}")
        print(f"Has 'valid': {hasattr(result, 'valid')}")
        print(f"Has 'errors': {hasattr(result, 'errors')}")
        
        if hasattr(result, 'valid'):
            print(f"Valid groups: {len(result.valid)}")
            for g in result.valid:
                print(f"  - {g.group_id if hasattr(g, 'group_id') else g}")
        
        if hasattr(result, 'errors'):
            print(f"Errors: {result.errors}")
    else:
        print("list_consumer_groups() not available")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
