# Debug & Utility Scripts

This directory contains debugging and utility scripts for troubleshooting and configuration.

## Files

### Debugging Scripts
- `debug_consumer_groups.py` - Debug consumer group visibility
- `debug_kafka_connection.py` - Debug Kafka connection issues
- `debug_member_details.py` - Inspect consumer group member details
- `debug_jmx_structure.py` - Debug JMX metric structure

### Exploration Scripts
- `check_producer_detection.py` - Explore producer detection limitations
- `explore_jmx_clients.py` - Explore available JMX metrics

### Configuration Scripts
- `configure_jmx.py` - Helper to configure JMX settings for a cluster

## Usage

```bash
cd server
.venv/bin/python debug/debug_consumer_groups.py
.venv/bin/python debug/configure_jmx.py 1 localhost 9999
```
