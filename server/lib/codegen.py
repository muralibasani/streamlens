"""
Generate sample producer/consumer code for a topic (Java and Python, with/without Schema Registry).
Used when user clicks a topic node and requests client code to copy.
"""


def _java_producer_plain(bootstrap_servers: str, topic: str) -> str:
    return f'''// Maven: org.apache.kafka:kafka-clients
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerExample {{
    public static void main(String[] args) {{
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "{bootstrap_servers}");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {{
            String topic = "{topic}";
            producer.send(new ProducerRecord<>(topic, "key1", "value1"));
            producer.flush();
        }}
    }}
}}
'''


def _java_producer_schema_registry(bootstrap_servers: str, topic: str, schema_registry_url: str) -> str:
    return f'''// Confluent: io.confluent:kafka-avro-serializer, io.confluent:kafka-schema-registry-client
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerAvroExample {{
    public static void main(String[] args) {{
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "{bootstrap_servers}");
        props.put("schema.registry.url", "{schema_registry_url}");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {{
            String topic = "{topic}";
            GenericRecord record = new GenericRecordBuilder(/* your Avro schema */).build();
            producer.send(new ProducerRecord<>(topic, "key1", record));
            producer.flush();
        }}
    }}
}}
'''


def _java_consumer_plain(bootstrap_servers: str, topic: str) -> str:
    return f'''// Maven: org.apache.kafka:kafka-clients
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerExample {{
    public static void main(String[] args) {{
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "{bootstrap_servers}");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {{
            consumer.subscribe(Collections.singletonList("{topic}"));
            while (true) {{
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(r -> System.out.println(r.key() + ": " + r.value()));
            }}
        }}
    }}
}}
'''


def _java_consumer_schema_registry(bootstrap_servers: str, topic: str, schema_registry_url: str) -> str:
    return f'''// Confluent: io.confluent:kafka-avro-serializer, io.confluent:kafka-schema-registry-client
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAvroExample {{
    public static void main(String[] args) {{
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "{bootstrap_servers}");
        props.put("schema.registry.url", "{schema_registry_url}");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {{
            consumer.subscribe(Collections.singletonList("{topic}"));
            while (true) {{
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(r -> System.out.println(r.value()));
            }}
        }}
    }}
}}
'''


def _python_producer_plain(bootstrap_servers: str, topic: str) -> str:
    return f'''# pip install confluent-kafka
from confluent_kafka import Producer

conf = {{'bootstrap.servers': '{bootstrap_servers}'}}
p = Producer(conf)

def delivery_callback(err, msg):
    if err:
        print(f'Delivery failed: {{err}}')
    else:
        print(f'Delivered to {{msg.topic()}} [{{msg.partition()}}] at offset {{msg.offset()}}')

topic = '{topic}'
p.produce(topic, key='key1', value='value1', callback=delivery_callback)
p.flush()
'''


def _python_producer_schema_registry(bootstrap_servers: str, topic: str, schema_registry_url: str) -> str:
    return f'''# pip install confluent-kafka confluent-kafka[avro]
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

schema_registry_url = '{schema_registry_url}'
conf = {{
    'bootstrap.servers': '{bootstrap_servers}',
    'schema.registry.url': schema_registry_url,
}}
# Value schema (Avro) - define or load from Schema Registry
value_schema = avro.loads("""{{"type": "record", "name": "Record", "fields": [{{"name": "id", "type": "string"}}]}}""")
producer = AvroProducer(conf, default_value_schema=value_schema)

topic = '{topic}'
producer.produce(topic=topic, value={{"id": "1"}}, key="key1")
producer.flush()
'''


def _python_consumer_plain(bootstrap_servers: str, topic: str) -> str:
    return f'''# pip install confluent-kafka
from confluent_kafka import Consumer

conf = {{
    'bootstrap.servers': '{bootstrap_servers}',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest',
}}
c = Consumer(conf)
c.subscribe(['{topic}'])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f'Error: {{msg.error()}}')
        continue
    print(f'{{msg.key()}}: {{msg.value()}}')
'''


def _python_consumer_schema_registry(bootstrap_servers: str, topic: str, schema_registry_url: str) -> str:
    return f'''# pip install confluent-kafka confluent-kafka[avro]
from confluent_kafka.avro import AvroConsumer

schema_registry_url = '{schema_registry_url}'
conf = {{
    'bootstrap.servers': '{bootstrap_servers}',
    'group.id': 'my-consumer-group',
    'schema.registry.url': schema_registry_url,
    'auto.offset.reset': 'earliest',
}}
c = AvroConsumer(conf)
c.subscribe(['{topic}'])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f'Error: {{msg.error()}}')
        continue
    print(msg.value())
'''


def _java_streams(bootstrap_servers: str, source_topic: str, output_topic: str) -> str:
    return f'''// Maven: org.apache.kafka:kafka-streams
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

/**
 * Kafka Streams example: read from "{source_topic}", filter, map, aggregate (count by key), write to "{output_topic}".
 */
public class StreamsExample {{

    public static void main(String[] args) {{
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "{bootstrap_servers}");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("{source_topic}");

        // Filter: keep only non-null, non-empty values
        KStream<String, String> filtered = source.filter((key, value) -> value != null && !value.isBlank());

        // Map values (e.g. uppercase) – optional transformation
        KStream<String, String> mapped = filtered.mapValues(value -> value.toUpperCase());

        // Aggregate: count messages per key, then write to output topic
        KTable<String, Long> counts = mapped
                .groupByKey()
                .count();

        counts.toStream().to("{output_topic}", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }}
}}
'''


def generate_code(
    bootstrap_servers: str,
    topic: str,
    client: str,  # "producer" | "consumer" | "streams"
    language: str,  # "java" | "python"
    schema_registry: bool,
    schema_registry_url: str | None = None,
    output_topic: str | None = None,  # for streams: target topic
) -> str:
    if client == "streams":
        out = output_topic or (topic.rstrip("/") + "-processed")
        return _java_streams(bootstrap_servers, topic, out)

    if schema_registry and not schema_registry_url:
        schema_registry_url = "http://localhost:8081"
    if not schema_registry:
        schema_registry_url = None

    if client == "producer":
        if language == "java":
            return _java_producer_schema_registry(bootstrap_servers, topic, schema_registry_url or "") if schema_registry else _java_producer_plain(bootstrap_servers, topic)
        else:
            return _python_producer_schema_registry(bootstrap_servers, topic, schema_registry_url or "") if schema_registry else _python_producer_plain(bootstrap_servers, topic)
    else:
        if language == "java":
            return _java_consumer_schema_registry(bootstrap_servers, topic, schema_registry_url or "") if schema_registry else _java_consumer_plain(bootstrap_servers, topic)
        else:
            return _python_consumer_schema_registry(bootstrap_servers, topic, schema_registry_url or "") if schema_registry else _python_consumer_plain(bootstrap_servers, topic)
