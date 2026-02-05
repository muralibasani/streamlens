#!/usr/bin/env python3
"""
Test script to verify topic configuration retrieval.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

print("=== Testing Topic Configuration Retrieval ===\n")

# Connect to Kafka
bootstrap = "localhost:9092"
admin = AdminClient({"bootstrap.servers": bootstrap})

# List topics
print("Available topics:")
metadata = admin.list_topics(timeout=5)
topics = [name for name in metadata.topics.keys() if not name.startswith("__")]
print(f"  {', '.join(topics[:5])}...\n" if len(topics) > 5 else f"  {', '.join(topics)}\n")

# Pick first topic to inspect
if not topics:
    print("❌ No topics found!")
    sys.exit(1)

topic_name = topics[0]
print(f"Inspecting topic: {topic_name}")
print("-" * 60)

# Get topic configuration
config_resource = ConfigResource(ResourceType.TOPIC, topic_name)
configs = admin.describe_configs([config_resource], request_timeout=10)

for res, future in configs.items():
    try:
        config_result = future.result()
        
        print("\nAll topic configurations:")
        print("=" * 60)
        
        # Get all configs
        all_configs = {}
        for entry in config_result.values():
            all_configs[entry.name] = {
                'value': entry.value,
                'source': str(entry.source) if hasattr(entry, 'source') else 'unknown',
                'is_default': entry.is_default if hasattr(entry, 'is_default') else None,
            }
        
        # Show key configs we care about
        key_configs = [
            'retention.ms',
            'retention.bytes',
            'max.message.bytes',
            'cleanup.policy',
            'segment.ms',
            'segment.bytes',
        ]
        
        print("\nKey Configurations:")
        print("-" * 60)
        for key in key_configs:
            if key in all_configs:
                config = all_configs[key]
                value = config['value']
                source = config['source']
                is_default = config.get('is_default')
                
                # Format value
                if key == 'retention.ms' and value and value != '-1':
                    ms = int(value)
                    days = ms // (1000 * 60 * 60 * 24)
                    hours = (ms % (1000 * 60 * 60 * 24)) // (1000 * 60 * 60)
                    display = f"{value} ms ({days}d {hours}h)"
                elif key == 'retention.bytes' and value:
                    if value == '-1':
                        display = f"{value} (Unlimited)"
                    else:
                        mb = int(value) / (1024 * 1024)
                        display = f"{value} bytes ({mb:.2f} MB)"
                elif key == 'max.message.bytes' and value:
                    mb = int(value) / (1024 * 1024)
                    display = f"{value} bytes ({mb:.2f} MB)"
                else:
                    display = value or 'N/A'
                
                default_marker = " [DEFAULT]" if is_default else " [EXPLICIT]"
                print(f"  {key:25s}: {display}{default_marker}")
                print(f"  {'':25s}  Source: {source}")
            else:
                print(f"  {key:25s}: NOT FOUND")
        
        print("\n" + "=" * 60)
        print("\n✅ Configuration retrieval working correctly!")
        print("\nNote:")
        print("  - Values marked [DEFAULT] are using broker defaults")
        print("  - Values marked [EXPLICIT] were explicitly set on the topic")
        print("  - retention.bytes = -1 means unlimited (default)")
        print("  - max.message.bytes default is usually 1 MB (1048588 bytes)")
        
    except Exception as e:
        print(f"❌ Error fetching config: {e}")
        import traceback
        traceback.print_exc()
