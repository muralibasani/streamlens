#!/usr/bin/env python3
"""
Test JMX connection to Kafka broker.
"""
import sys

def test_jmx(jmx_host="localhost", jmx_port=9999):
    print(f"=== Testing JMX Connection to {jmx_host}:{jmx_port} ===\n")
    
    try:
        from jmxquery import JMXConnection, JMXQuery
        print("✅ jmxquery library is installed")
    except ImportError:
        print("❌ jmxquery not installed!")
        print("Run: uv sync  or  pip install jmxquery")
        return
    
    try:
        print(f"\n1. Connecting to JMX at {jmx_host}:{jmx_port}...")
        jmx_connection = JMXConnection(f"service:jmx:rmi:///jndi/rmi://{jmx_host}:{jmx_port}/jmxrmi")
        print("✅ JMX connection successful!")
        
        print("\n2. Querying BrokerTopicMetrics...")
        topic_metrics_query = JMXQuery(
            "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*",
            metric_name="Count",
            metric_labels={"topic": "topic"}
        )
        
        metrics = jmx_connection.query([topic_metrics_query])
        
        if not metrics:
            print("⚠️  No metrics found (broker might not have received any messages yet)")
            print("   Try sending a message to a topic first")
        else:
            print(f"✅ Found {len(metrics)} topic metrics")
            
            active_topics = []
            for metric in metrics:
                topic = metric.metric_labels.get("topic", "unknown")
                try:
                    # Handle various value formats
                    if metric.value is None:
                        value = 0
                    elif isinstance(metric.value, (int, float)):
                        value = float(metric.value)
                    else:
                        # Try to extract number from string like "210.0 messages"
                        value_str = str(metric.value).split()[0]
                        value = float(value_str)
                except (ValueError, IndexError):
                    value = 0
                
                if value > 0:
                    print(f"   ⚡ {topic}: {value} messages/sec (ACTIVE)")
                    active_topics.append(topic)
                else:
                    print(f"   ⏸️  {topic}: {value} messages/sec (idle)")
            
            if active_topics:
                print(f"\n✅ Found {len(active_topics)} active producer(s)!")
                print("   These will show as ⚡ JMX producers in the UI")
            else:
                print("\n⚠️  No active producers detected")
                print("   Send messages to a topic to see JMX producers")
        
        print("\n3. Querying Produce RequestMetrics...")
        try:
            request_query = JMXQuery(
                "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce",
                metric_name="Count"
            )
            request_metrics = jmx_connection.query([request_query])
            
            if request_metrics:
                rate = float(request_metrics[0].value) if request_metrics[0].value else 0
                print(f"   Produce requests/sec: {rate}")
        except Exception as e:
            print(f"   Could not query request metrics: {e}")
        
        print("\n" + "="*60)
        print("JMX TEST SUCCESSFUL!")
        print("="*60)
        print("\nNext steps:")
        print("1. Configure cluster with JMX: python configure_jmx.py")
        print("2. Restart backend server")
        print("3. Send messages to topics")
        print("4. Click 'Sync' in UI")
        print("5. See ⚡ JMX producers!")
        
    except Exception as e:
        print(f"\n❌ JMX connection failed: {e}")
        print("\nTroubleshooting:")
        print("1. Is JMX_PORT set? Check with: echo $JMX_PORT")
        print("2. Is Kafka running? Check with: ps aux | grep kafka")
        print("3. Is port accessible? Check with: telnet localhost 9999")
        print("4. Check Kafka logs for JMX startup message")
        return 1
    
    return 0

if __name__ == "__main__":
    jmx_host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    jmx_port = int(sys.argv[2]) if len(sys.argv) > 2 else 9999
    
    sys.exit(test_jmx(jmx_host, jmx_port))
