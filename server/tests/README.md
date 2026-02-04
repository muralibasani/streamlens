# Test Scripts

This directory contains test scripts for verifying various Kafka functionality.

## Files

- `test_consumer_query.py` - Test consumer group detection
- `test_jmx.py` - Test JMX connection and producer detection
- `test_list_groups.py` - Test consumer group listing

## Running Tests

```bash
cd server
.venv/bin/python tests/test_jmx.py
.venv/bin/python tests/test_consumer_query.py
```
