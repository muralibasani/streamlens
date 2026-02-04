#!/usr/bin/env python3
"""Debug the structure of JMX query results."""

from jmxquery import JMXConnection, JMXQuery

jmx_host = "localhost"
jmx_port = 9999

print(f"=== Debugging JMX Query Structure ===\n")
print(f"Connecting to {jmx_host}:{jmx_port}...\n")

jmx_connection = JMXConnection(f"service:jmx:rmi:///jndi/rmi://{jmx_host}:{jmx_port}/jmxrmi")

topic_metrics_query = JMXQuery(
    "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*",
    metric_name="Count"
)

metrics = jmx_connection.query([topic_metrics_query])

print(f"Found {len(metrics)} metrics\n")

for i, metric in enumerate(metrics[:3]):  # Show first 3 metrics
    print(f"Metric {i+1}:")
    print(f"  Type: {type(metric)}")
    print(f"  Attributes: {dir(metric)}")
    print(f"  mbean: {getattr(metric, 'mbean', 'N/A')}")
    print(f"  attribute: {getattr(metric, 'attribute', 'N/A')}")
    print(f"  value: {getattr(metric, 'value', 'N/A')}")
    print(f"  metric_labels: {getattr(metric, 'metric_labels', 'N/A')}")
    print(f"  to_query_string: {metric.to_query_string() if hasattr(metric, 'to_query_string') else 'N/A'}")
    print()

# Try to extract topic name
import re
print("Topic extraction tests:")
for i, metric in enumerate(metrics[:5]):
    # Try different ways to get the MBean name
    query_string = metric.to_query_string() if hasattr(metric, 'to_query_string') else ''
    mbean_name = getattr(metric, 'mBeanName', '')
    
    # Extract from to_query_string
    match1 = re.search(r'topic=([^,/\]]+)', query_string)
    topic_from_query = match1.group(1) if match1 else "NOT FOUND"
    
    # Extract from mBeanName
    match2 = re.search(r'topic=([^,/\]]+)', mbean_name)
    topic_from_mbean = match2.group(1) if match2 else "NOT FOUND"
    
    value = getattr(metric, 'value', 0)
    attribute = getattr(metric, 'attribute', 'N/A')
    
    print(f"  Metric {i+1}:")
    print(f"    query_string: {query_string[:80]}...")
    print(f"    topic (from query): '{topic_from_query}'")
    print(f"    topic (from mBeanName): '{topic_from_mbean}'")
    print(f"    attribute: {attribute}, value: {value}")
    print()
