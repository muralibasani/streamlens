"""Unit tests for server/src/codegen.py."""
from src.codegen import generate_code


class TestJavaProducer:
    def test_java_producer_plain(self):
        code = generate_code(
            bootstrap_servers="localhost:9092",
            topic="orders",
            client="producer",
            language="java",
            schema_registry=False,
        )
        assert "ProducerExample" in code
        assert "schema.registry.url" not in code

    def test_java_producer_with_schema_registry(self):
        code = generate_code(
            bootstrap_servers="localhost:9092",
            topic="orders",
            client="producer",
            language="java",
            schema_registry=True,
        )
        assert "ProducerExample" not in code
        assert "ProducerAvroExample" in code or "schema.registry.url" in code
        assert "schema.registry.url" in code


class TestJavaConsumer:
    def test_java_consumer_plain(self):
        code = generate_code(
            bootstrap_servers="localhost:9092",
            topic="orders",
            client="consumer",
            language="java",
            schema_registry=False,
        )
        assert "ConsumerExample" in code
        assert "schema.registry.url" not in code

    def test_java_consumer_with_schema_registry(self):
        code = generate_code(
            bootstrap_servers="localhost:9092",
            topic="orders",
            client="consumer",
            language="java",
            schema_registry=True,
        )
        assert "ConsumerAvroExample" in code or "schema.registry.url" in code
        assert "schema.registry.url" in code


class TestPythonProducer:
    def test_python_producer_plain(self):
        code = generate_code(
            bootstrap_servers="localhost:9092",
            topic="orders",
            client="producer",
            language="python",
            schema_registry=False,
        )
        assert "confluent_kafka" in code
        assert "AvroProducer" not in code

    def test_python_producer_with_schema_registry(self):
        code = generate_code(
            bootstrap_servers="localhost:9092",
            topic="orders",
            client="producer",
            language="python",
            schema_registry=True,
        )
        assert "AvroProducer" in code


class TestPythonConsumer:
    def test_python_consumer_plain(self):
        code = generate_code(
            bootstrap_servers="localhost:9092",
            topic="orders",
            client="consumer",
            language="python",
            schema_registry=False,
        )
        assert "Consumer" in code
        assert "AvroConsumer" not in code

    def test_python_consumer_with_schema_registry(self):
        code = generate_code(
            bootstrap_servers="localhost:9092",
            topic="orders",
            client="consumer",
            language="python",
            schema_registry=True,
        )
        assert "AvroConsumer" in code


class TestStreams:
    def test_streams_contains_streams_builder_and_output_topic(self):
        code = generate_code(
            bootstrap_servers="localhost:9092",
            topic="input-topic",
            client="streams",
            language="java",
            schema_registry=False,
            output_topic="my-output",
        )
        assert "StreamsBuilder" in code
        assert "my-output" in code
        assert "output_topic" in code or "output" in code

    def test_streams_default_output_topic(self):
        code = generate_code(
            bootstrap_servers="localhost:9092",
            topic="orders",
            client="streams",
            language="java",
            schema_registry=False,
        )
        assert "StreamsBuilder" in code
        assert "orders-processed" in code


class TestSchemaRegistryDefaultUrl:
    def test_schema_registry_true_no_url_uses_default(self):
        """When schema_registry=True but no URL, uses http://localhost:8081."""
        code = generate_code(
            bootstrap_servers="localhost:9092",
            topic="orders",
            client="producer",
            language="java",
            schema_registry=True,
        )
        assert "http://localhost:8081" in code
