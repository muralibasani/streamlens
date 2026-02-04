#!/usr/bin/env python3
"""
Explore JMX metrics to find producer/client IDs.
"""

from jmxquery import JMXConnection, JMXQuery

jmx_host = "localhost"
jmx_port = 9999

print(f"=== Exploring JMX Metrics for Producer/Client IDs ===\n")
print(f"Connecting to {jmx_host}:{jmx_port}...\n")

jmx_connection = JMXConnection(f"service:jmx:rmi:///jndi/rmi://{jmx_host}:{jmx_port}/jmxrmi")

# Try different metric queries that might contain client information
queries_to_try = [
    ("Producer Metrics", "kafka.producer:type=producer-metrics,client-id=*"),
    ("Producer Node Metrics", "kafka.producer:type=producer-node-metrics,client-id=*,node-id=*"),
    ("Producer Topic Metrics", "kafka.producer:type=producer-topic-metrics,client-id=*,topic=*"),
    ("Fetch Session Cache", "kafka.server:type=FetchSessionCache,*"),
    ("Broker Topic Metrics with Client", "kafka.server:type=BrokerTopicMetrics,name=TotalProduceRequestsPerSec,*"),
    ("Client Quota Metrics", "kafka.server:type=ClientQuotaMetrics,*"),
    ("Request Metrics", "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce,*"),
    ("Request Handler Avg Idle", "kafka.server:type=KafkaRequestHandlerPool,*"),
    ("Delayed Producer Metrics", "kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=Produce"),
]

for name, query_pattern in queries_to_try:
    print(f"\n{'='*60}")
    print(f"Query: {name}")
    print(f"Pattern: {query_pattern}")
    print('='*60)
    
    try:
        query = JMXQuery(query_pattern, metric_name="*")
        metrics = jmx_connection.query([query])
        
        if not metrics:
            print("  ❌ No metrics found")
        else:
            print(f"  ✅ Found {len(metrics)} metric(s)")
            
            # Show first 3 examples
            for i, metric in enumerate(metrics[:3]):
                mbean = getattr(metric, 'mBeanName', '') or metric.to_query_string()
                value = getattr(metric, 'value', 'N/A')
                attribute = getattr(metric, 'attribute', 'N/A')
                
                print(f"\n  Example {i+1}:")
                print(f"    MBean: {mbean[:100]}...")
                print(f"    Attribute: {attribute}")
                print(f"    Value: {value}")
                
                # Try to extract client-id if present
                import re
                client_match = re.search(r'client-id=([^,\]]+)', mbean)
                if client_match:
                    print(f"    🎯 Client ID: {client_match.group(1)}")
                
                topic_match = re.search(r'topic=([^,\]]+)', mbean)
                if topic_match:
                    print(f"    🎯 Topic: {topic_match.group(1)}")
            
            if len(metrics) > 3:
                print(f"\n  ... and {len(metrics) - 3} more")
                
    except Exception as e:
        print(f"  ❌ Error: {e}")

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print("""
Standard Kafka JMX metrics have limited producer tracking:
- Topic-level metrics show activity but not which client
- Client metrics (kafka.producer:*) only work if producer is on same JVM
- Network metrics show requests but not client-to-topic mapping

For detailed producer tracking, you would need:
1. Custom interceptors (requires code changes)
2. Application-level metrics (Prometheus, etc.)
3. Kafka audit logging (enterprise feature)
""")
