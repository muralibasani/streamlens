"""
Fetch real Kafka cluster state: topics, consumer groups, connectors (optional), schemas (optional).
Uses confluent-kafka AdminClient, Kafka Connect REST API, and Schema Registry REST API.
"""
import logging
import os
import time
from pathlib import Path
from typing import Any

import httpx
import yaml
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaException
from confluent_kafka._model import ConsumerGroupTopicPartitions

logger = logging.getLogger(__name__)


class KafkaService:
    """Fetches real cluster state from Kafka broker, Connect, and Schema Registry."""

    def check_cluster_health(self, bootstrap_servers: str) -> dict[str, Any]:
        """
        Check if Kafka cluster is reachable and detect controller mode.
        Returns: {"online": bool, "error": str | None, "clusterMode": "kraft" | "zookeeper" | None}
        """
        if not bootstrap_servers:
            return {"online": False, "error": "No bootstrap servers configured", "clusterMode": None}

        bootstrap_list = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
        if not bootstrap_list:
            return {"online": False, "error": "Invalid bootstrap servers", "clusterMode": None}

        try:
            admin = AdminClient({"bootstrap.servers": ",".join(bootstrap_list)})
            metadata = admin.list_topics(timeout=5)
            if metadata is not None:
                # KRaft clusters expose __cluster_metadata in topic list; Zookeeper mode does not
                mode: str | None = None
                if getattr(metadata, "topics", None):
                    mode = "kraft" if "__cluster_metadata" in metadata.topics else "zookeeper"
                return {"online": True, "error": None, "clusterMode": mode}
            return {"online": False, "error": "Failed to retrieve cluster metadata", "clusterMode": None}
        except Exception as e:
            error_msg = str(e)
            if "timed out" in error_msg.lower():
                error_msg = "Connection timeout - cluster unreachable"
            elif "failed to resolve" in error_msg.lower():
                error_msg = "Cannot resolve bootstrap servers"
            return {"online": False, "error": error_msg, "clusterMode": None}

    def fetch_system_state(self, cluster: dict[str, Any]) -> dict[str, Any]:
        """
        cluster: dict with bootstrapServers, schemaRegistryUrl?, connectUrl?, jmxHost?, jmxPort?
        Returns: topics, consumers (from consumer groups), connectors?, schemas?
        """
        bootstrap = cluster.get("bootstrapServers") or ""
        if not bootstrap:
            return self._empty_state()

        bootstrap_list = [s.strip() for s in bootstrap.split(",") if s.strip()]
        if not bootstrap_list:
            return self._empty_state()

        state: dict[str, Any] = {
            "topics": [],
            "producers": [],
            "consumers": [],
            "streams": [],
            "connectors": [],
            "schemas": [],
            "acls": [],
        }

        try:
            admin = AdminClient({"bootstrap.servers": ",".join(bootstrap_list)})
            # Real topics from broker
            state["topics"] = self._fetch_topics(admin)
            # Consumer groups -> consumers (group id, consumes from topics)
            state["consumers"] = self._fetch_consumer_groups(admin, ",".join(bootstrap_list))
            
            # Producers: Try multiple sources
            producers = []

            # Source 1: JMX metrics (real-time active producers)
            jmx_host = cluster.get("jmxHost")
            jmx_port = cluster.get("jmxPort")
            if jmx_host and jmx_port:
                jmx_producers = self._fetch_jmx_producers(jmx_host, jmx_port)
                producers.extend(jmx_producers)
            
            # Source 2: ACL-based potential producers (if ACLs enabled)
            acl_producers = self._fetch_acl_producers(admin)
            producers.extend(acl_producers)
            
            state["producers"] = producers
            logger.info("Topology state: %d producers (will appear in UI after Sync)", len(producers))
            
            # Topic ACLs: bindings for TOPIC resource (for ACL nodes in topology)
            topic_names_for_acl = [t["name"] for t in state["topics"] if not (t.get("name") or "").startswith("__")]
            state["acls"] = self._fetch_topic_acls(admin, topic_names_for_acl)
            
            # Load configured Kafka Streams applications (from streams.yaml)
            state["streams"] = self._load_streams_config()
            
        except Exception as e:
            logger.exception("Kafka broker error: %s", e)
            raise RuntimeError(f"Cannot connect to Kafka at {bootstrap}: {e}") from e

        # Optional: Kafka Connect connectors
        connect_url = (cluster.get("connectUrl") or "").strip().rstrip("/")
        if connect_url:
            try:
                state["connectors"] = self._fetch_connectors(connect_url)
            except Exception as e:
                logger.warning("Kafka Connect unreachable at %s: %s", connect_url, e)

        # Optional: Schema Registry subjects
        schema_url = (cluster.get("schemaRegistryUrl") or "").strip().rstrip("/")
        if schema_url:
            try:
                state["schemas"] = self._fetch_schemas(schema_url)
            except Exception as e:
                logger.warning("Schema Registry unreachable at %s: %s", schema_url, e)

        return state

    def _empty_state(self) -> dict[str, Any]:
        return {
            "topics": [],
            "producers": [],
            "consumers": [],
            "streams": [],
            "connectors": [],
            "schemas": [],
            "acls": [],
        }

    def _fetch_topics(self, admin: AdminClient) -> list[dict[str, Any]]:
        topics = []
        try:
            metadata = admin.list_topics(timeout=10)
            for name, t in metadata.topics.items():
                if name.startswith("__"):
                    continue
                partitions = len(t.partitions) if t.partitions else 0
                replication = 0
                if t.partitions:
                    for p in t.partitions.values():
                        if hasattr(p, "replicas") and p.replicas:
                            replication = len(p.replicas)
                            break
                topics.append({
                    "name": name,
                    "partitions": partitions,
                    "replication": replication,
                })
        except Exception as e:
            logger.exception("list_topics failed: %s", e)
            raise
        return topics

    def _fetch_consumer_groups(self, admin: AdminClient, bootstrap_servers: str) -> list[dict[str, Any]]:
        """Fetch consumer groups and the topics they consume from (auto-discovered)."""
        consumers = []
        try:
            from confluent_kafka import Consumer, TopicPartition
            
            # Use list_groups to get full metadata (including member info)
            # This is more informative than list_consumer_groups for subscription detection
            groups = admin.list_groups(timeout=10)
            if not groups:
                logger.info("No groups found")
                return consumers
            
            # Filter to consumer groups and extract full metadata
            consumer_group_metadata = {}
            for group in groups:
                protocol_type = getattr(group, 'protocol_type', '')
                if protocol_type == 'consumer':
                    consumer_group_metadata[group.id] = group
            
            group_ids = list(consumer_group_metadata.keys())
            logger.info("Found %d consumer groups: %s", len(group_ids), group_ids)
            
            if not group_ids:
                return consumers
            
            for group_id in group_ids:
                topics_consumed = set()
                group_metadata = consumer_group_metadata.get(group_id)
                
                # Method 1: Extract topics from member metadata and assignment
                if group_metadata and hasattr(group_metadata, 'members') and group_metadata.members:
                    logger.info(f"Group '{group_id}' has {len(group_metadata.members)} active member(s)")
                    
                    for member in group_metadata.members:
                        # Get all actual topics from cluster for validation
                        all_topics = admin.list_topics(timeout=5).topics.keys()
                        
                        # Try to parse 'metadata' (subscription info)
                        if hasattr(member, 'metadata') and member.metadata:
                            try:
                                metadata_bytes = member.metadata
                                # Decode to find subscribed topics
                                decoded = metadata_bytes.decode('utf-8', errors='ignore')
                                
                                # Look for topic names in the decoded metadata
                                for topic in all_topics:
                                    if not topic.startswith('__') and topic in decoded:
                                        topics_consumed.add(topic)
                                        logger.info(f"  Found topic '{topic}' in member metadata (subscription)")
                                        
                            except Exception as e:
                                logger.debug(f"Could not parse metadata: {e}")
                        
                        # Try to parse 'assignment' (assigned partitions)
                        if hasattr(member, 'assignment') and member.assignment:
                            try:
                                assignment_bytes = member.assignment
                                # Decode to find assigned topics
                                decoded = assignment_bytes.decode('utf-8', errors='ignore')
                                
                                # Look for topic names in the decoded assignment
                                for topic in all_topics:
                                    if not topic.startswith('__') and topic in decoded:
                                        topics_consumed.add(topic)
                                        logger.info(f"  Found topic '{topic}' in member assignment")
                                        
                            except Exception as e:
                                logger.debug(f"Could not parse assignment: {e}")
                
                # Method 2: Check committed offsets (for groups that have consumed messages)
                if not topics_consumed:
                    try:
                        # Create a temporary consumer to query offsets
                        temp_consumer = Consumer({
                            'bootstrap.servers': bootstrap_servers,
                            'group.id': f'_temp_query_{group_id}',  # Different group to avoid conflicts
                            'enable.auto.commit': False,
                        })
                        
                        # Get list of committed topic partitions for the target group
                        # We need to get cluster metadata first to know all topics
                        cluster_metadata = temp_consumer.list_topics(timeout=5)
                        
                        # Check each topic to see if this group has committed offsets
                        for topic_name in cluster_metadata.topics.keys():
                            if topic_name.startswith('__'):
                                continue  # Skip internal topics
                            
                            # Get partitions for this topic
                            topic_metadata = cluster_metadata.topics[topic_name]
                            partitions = list(topic_metadata.partitions.keys())
                            
                            if not partitions:
                                continue
                            
                            # Check if the group has any committed offsets for this topic
                            tps = [TopicPartition(topic_name, p) for p in partitions]
                            
                            # Get committed offsets for the actual group
                            committed = temp_consumer.committed(tps, timeout=2)
                            
                            # If any partition has a valid offset, the group consumes from this topic
                            for tp in committed:
                                if tp.offset >= 0:  # Valid offset
                                    topics_consumed.add(topic_name)
                                    break
                        
                        temp_consumer.close()
                        
                    except Exception as e:
                        logger.debug(f"Could not query offsets for group {group_id}: {e}")
                
                # Add the group
                if topics_consumed:
                    logger.info(f"Group '{group_id}' consumes from: {sorted(topics_consumed)}")
                    
                    # Detect if this is likely a Kafka Streams application
                    # Based on consumer group naming patterns
                    is_streams = self._is_likely_streams_app(group_id)
                    
                    consumers.append({
                        "id": f"group:{group_id}",
                        "consumesFrom": sorted(topics_consumed),
                        "source": "auto-discovered",
                        "isStreams": is_streams,  # Flag for potential streams app
                    })
                else:
                    # Add group even without topic info (might be new/empty)
                    logger.info(f"Group '{group_id}' found but no committed offsets detected")
                    consumers.append({
                        "id": f"group:{group_id}",
                        "consumesFrom": [],
                        "source": "auto-discovered",
                        "isStreams": False,
                    })
                    
        except Exception as e:
            logger.warning("consumer groups fetch failed: %s", e)
            import traceback
            logger.warning(traceback.format_exc())
        
        return consumers
    
    def _is_likely_streams_app(self, group_id: str) -> bool:
        """
        Heuristic to detect if a consumer group is likely a Kafka Streams application.
        Based on common naming patterns.
        """
        group_lower = group_id.lower()
        
        # Common Kafka Streams patterns
        streams_patterns = [
            'stream',
            'streams',
            'kstream',
            'processor',
            'transformer',
            'aggregator',
            'enricher',
            '-application',  # Streams apps often end with -application
        ]
        
        for pattern in streams_patterns:
            if pattern in group_lower:
                return True
        
        return False

    def _fetch_jmx_producers(self, jmx_host: str, jmx_port: int) -> list[dict[str, Any]]:
        """
        Fetch active producers from Kafka broker JMX metrics.
        Queries BrokerTopicMetrics to find which clients are actively producing.
        """
        producers = []
        try:
            from jmxquery import JMXConnection, JMXQuery
            
            logger.info(f"Connecting to JMX at {jmx_host}:{jmx_port}")
            
            # Connect to JMX
            jmx_connection = JMXConnection(f"service:jmx:rmi:///jndi/rmi://{jmx_host}:{jmx_port}/jmxrmi")
            
            # Query 1: Get topic-level produce metrics
            # This shows MessagesInPerSec for each topic
            # Note: jmxquery extracts properties from ObjectName automatically
            topic_metrics_query = JMXQuery(
                "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*",
                metric_name="Count"
            )
            
            metrics = jmx_connection.query([topic_metrics_query])
            logger.debug(f"JMX: Retrieved {len(metrics)} metric(s)")
            
            # Parse metrics to find active topics (those receiving messages)
            # JMX returns multiple attributes per topic (Count, OneMinuteRate, etc.)
            # We only need to check one attribute (Count) per topic
            active_topics = set()
            import re
            
            for metric in metrics:
                try:
                    # Only process "Count" attribute to avoid duplicates
                    attribute = getattr(metric, 'attribute', '')
                    if attribute != 'Count':
                        continue
                    
                    # Extract topic name from mBeanName or to_query_string
                    mbean_name = getattr(metric, 'mBeanName', '')
                    if not mbean_name and hasattr(metric, 'to_query_string'):
                        mbean_name = metric.to_query_string()
                    
                    # Parse topic from: kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=<topicname>
                    match = re.search(r'topic=([^,/\]]+)', mbean_name)
                    if not match:
                        logger.debug(f"Could not extract topic from: {mbean_name}")
                        continue
                    
                    topic = match.group(1)
                    
                    # Skip internal topics
                    if topic.startswith("__") or topic == "*":
                        continue
                    
                    # Handle various value formats
                    if metric.value is None:
                        value = 0
                    elif isinstance(metric.value, (int, float)):
                        value = float(metric.value)
                    else:
                        # Try to extract number from string
                        value_str = str(metric.value).split()[0]
                        value = float(value_str)
                    
                    # Only add if there's actual message flow
                    if value > 0:
                        active_topics.add(topic)
                        logger.info(f"JMX: Topic '{topic}' has active producers (count: {value})")
                        
                except (ValueError, IndexError, AttributeError) as e:
                    logger.debug(f"Could not parse metric: {e}")
                    continue
            
            # Query 2: Try to get client connection metrics
            # This is broker-dependent and might not always be available
            try:
                client_query = JMXQuery(
                    "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce",
                    metric_name="Count"
                )
                client_metrics = jmx_connection.query([client_query])
                
                # Unfortunately, client IDs are not directly available in standard JMX metrics
                # We can only determine that produce requests are happening
                if client_metrics and client_metrics[0].value and float(client_metrics[0].value) > 0:
                    logger.info(f"JMX: Active produce requests detected (rate: {client_metrics[0].value})")
            except Exception as e:
                logger.debug(f"Could not query client metrics: {e}")
            
            # Create producer entries for active topics
            # Note: We can't reliably get client IDs from standard JMX metrics
            # So we'll create a synthetic "active-producer" entry per topic
            if active_topics:
                logger.info(f"JMX detected producers for {len(active_topics)} topic(s)")
                for topic in sorted(active_topics):
                    producers.append({
                        "id": f"jmx:active-producer:{topic}",
                        "producesTo": [topic],
                        "source": "jmx",
                        "label": f"Active Producer → {topic}",
                    })
            else:
                logger.info("JMX: No active producers detected")
                
        except ImportError:
            logger.warning("jmxquery library not installed. Install with: pip install jmxquery")
        except Exception as e:
            logger.warning(f"JMX producer fetch failed (JMX might not be enabled): {e}")
            logger.debug("To enable JMX on Kafka, set: JMX_PORT=9999 before starting Kafka")
        
        return producers
    
    def _fetch_acl_producers(self, admin: AdminClient) -> list[dict[str, Any]]:
        """
        Fetch potential producers from ACLs (if ACLs are enabled).
        Returns principals with WRITE permission on topics.
        """
        producers = []
        try:
            from confluent_kafka.admin import AclBinding, AclBindingFilter, ResourceType, ResourcePatternType, AclOperation, AclPermissionType
            
            # Create a filter to get all ACLs
            acl_filter = AclBindingFilter(
                restype=ResourceType.TOPIC,
                name=None,  # All topics
                resource_pattern_type=ResourcePatternType.ANY,
                principal=None,  # All principals
                host=None,
                operation=AclOperation.WRITE,
                permission_type=AclPermissionType.ALLOW
            )
            
            # Fetch ACLs
            result = admin.describe_acls(acl_filter, request_timeout=10)
            acls = result.result()
            
            # Group by principal and collect topics they can write to
            principal_topics = {}
            for acl in acls:
                principal = acl.principal
                topic = acl.resource_name
                
                # Skip internal topics
                if topic.startswith("__"):
                    continue
                
                # Clean up principal name (remove "User:" prefix)
                clean_principal = principal.replace("User:", "").replace("ServiceAccount:", "")
                
                if clean_principal not in principal_topics:
                    principal_topics[clean_principal] = set()
                principal_topics[clean_principal].add(topic)
            
            # Create producer entries
            for principal, topics in principal_topics.items():
                if topics:
                    producers.append({
                        "id": f"acl:{principal}",
                        "producesTo": sorted(topics),
                        "source": "acl",  # Mark as ACL-based
                        "principal": principal,
                    })
                    
            logger.info("Found %d potential producers from ACLs", len(producers))
        except ImportError:
            logger.debug("ACL classes not available in confluent-kafka version")
        except Exception as e:
            logger.debug("ACL fetch failed (ACLs might not be enabled): %s", e)
        
        return producers

    def _fetch_topic_acls(self, admin: AdminClient, topic_names: list[str] | None = None) -> list[dict[str, Any]]:
        """
        Fetch all ACL bindings for TOPIC resources (for ACL nodes in topology).
        Returns list of dicts: topic, principal, host, operation, permissionType.
        Tries filter with name=None first; if that fails or returns nothing, queries per topic.
        """
        acls: list[dict[str, Any]] = []
        topic_names = topic_names or []

        def parse_binding(acl: Any) -> dict[str, Any] | None:
            topic = getattr(acl, "name", None) or getattr(acl, "resource_name", None)
            if not topic or (isinstance(topic, str) and topic.startswith("__")):
                return None
            principal = getattr(acl, "principal", "") or ""
            host = getattr(acl, "host", "") or ""
            op = getattr(acl, "operation", None)
            perm = getattr(acl, "permission_type", None)
            operation = op.name if hasattr(op, "name") else str(op) if op else "UNKNOWN"
            permission_type = perm.name if hasattr(perm, "name") else str(perm) if perm else "UNKNOWN"
            return {
                "topic": topic,
                "principal": principal,
                "host": host,
                "operation": operation,
                "permissionType": permission_type,
            }

        try:
            from confluent_kafka.admin import (
                AclBindingFilter,
                ResourceType,
                ResourcePatternType,
                AclOperation,
                AclPermissionType,
            )

            # Strategy 1: filter with name=None (match any topic) if the API allows it
            try:
                acl_filter = AclBindingFilter(
                    restype=ResourceType.TOPIC,
                    name=None,
                    resource_pattern_type=ResourcePatternType.ANY,
                    principal=None,
                    host=None,
                    operation=AclOperation.ANY,
                    permission_type=AclPermissionType.ANY,
                )
                result = admin.describe_acls(acl_filter, request_timeout=10)
                bindings = result.result()
                for acl in bindings:
                    parsed = parse_binding(acl)
                    if parsed:
                        acls.append(parsed)
                if acls:
                    logger.info("Found %d topic ACL bindings (match-any filter)", len(acls))
                    return acls
            except (TypeError, ValueError) as e:
                logger.debug("Match-any ACL filter not supported (%s), trying per-topic", e)

            # Strategy 2: describe ACLs per topic (works when filter with None is rejected or returns nothing)
            if not topic_names:
                try:
                    metadata = admin.list_topics(timeout=5)
                    if getattr(metadata, "topics", None):
                        topic_names = [n for n in metadata.topics.keys() if n and not n.startswith("__")]
                except Exception as e:
                    logger.debug("list_topics for ACL fallback: %s", e)
            seen: set[tuple[str, str, str, str, str]] = set()
            for topic in topic_names:
                if not topic or topic.startswith("__"):
                    continue
                try:
                    acl_filter = AclBindingFilter(
                        restype=ResourceType.TOPIC,
                        name=topic,
                        resource_pattern_type=ResourcePatternType.LITERAL,
                        principal=None,
                        host=None,
                        operation=AclOperation.ANY,
                        permission_type=AclPermissionType.ANY,
                    )
                    result = admin.describe_acls(acl_filter, request_timeout=5)
                    bindings = result.result()
                    for acl in bindings:
                        parsed = parse_binding(acl)
                        if parsed:
                            key = (parsed["topic"], parsed["principal"], parsed["host"], parsed["operation"], parsed["permissionType"])
                            if key not in seen:
                                seen.add(key)
                                acls.append(parsed)
                except Exception as e:
                    logger.debug("ACL describe for topic %s: %s", topic, e)
            if acls:
                logger.info("Found %d topic ACL bindings (per-topic)", len(acls))
        except ImportError as e:
            logger.debug("ACL classes not available in confluent-kafka version: %s", e)
        except Exception as e:
            logger.warning("Topic ACL fetch failed: %s", e)
        return acls

    def fetch_connector_details(self, connect_url: str, connector_name: str) -> dict[str, Any]:
        """
        Fetch detailed configuration for a specific connector.
        Masks sensitive configuration values (passwords, keys, secrets).
        """
        try:
            with httpx.Client(timeout=10.0) as client:
                r = client.get(f"{connect_url}/connectors/{connector_name}")
                r.raise_for_status()
                info = r.json()
                
                # Mask sensitive config keys
                config = info.get("config", {})
                masked_config = {}
                
                # List of config keys that contain sensitive information
                sensitive_keywords = [
                    'password', 'passwd', 'pwd',
                    'secret', 'key', 'token',
                    'credential', 'auth',
                    'ssl.key', 'ssl.truststore.password', 'ssl.keystore.password',
                    'sasl.jaas.config', 'connection.password',
                    'aws.secret', 'azure.client.secret',
                    'api.key', 'api.secret'
                ]
                
                for key, value in config.items():
                    # Check if the key contains any sensitive keywords
                    key_lower = key.lower()
                    is_sensitive = any(keyword in key_lower for keyword in sensitive_keywords)
                    
                    if is_sensitive and value:
                        masked_config[key] = "********"
                    else:
                        masked_config[key] = value
                
                return {
                    "name": info.get("name"),
                    "type": info.get("type", "unknown"),
                    "config": masked_config,
                    "tasks": info.get("tasks", []),
                    "connectorClass": config.get("connector.class", "N/A"),
                }
        except Exception as e:
            logger.error(f"Failed to fetch connector details for {connector_name}: {e}")
            raise RuntimeError(f"Could not fetch connector details: {str(e)}") from e
    
    def _fetch_connectors(self, connect_url: str) -> list[dict[str, Any]]:
        connectors = []
        with httpx.Client(timeout=10.0) as client:
            r = client.get(f"{connect_url}/connectors")
            r.raise_for_status()
            names = r.json()
            for name in names:
                try:
                    r2 = client.get(f"{connect_url}/connectors/{name}")
                    r2.raise_for_status()
                    info = r2.json()
                    config = info.get("config", {})
                    connector_class = config.get("connector.class", "")
                    # Common config keys for topics
                    topics_conf = (
                        config.get("topics") or config.get("topics.regex")
                        or config.get("topic") or config.get("topic.regex")
                        or ""
                    )
                    topic_list = [t.strip() for t in topics_conf.split(",") if t.strip()] if isinstance(topics_conf, str) else []
                    if not topic_list and "topic" in config:
                        topic_list = [config["topic"].strip()]
                    # Sink = consumes from Kafka; source = produces to Kafka
                    is_sink = "sink" in connector_class.lower() or "Sink" in info.get("type", "")
                    connector_type = "sink" if is_sink else "source"
                    if topic_list:
                        for topic in topic_list[:5]:  # limit for display
                            connectors.append({
                                "id": f"connect:{name}",
                                "type": connector_type,
                                "topic": topic,
                            })
                    else:
                        connectors.append({"id": f"connect:{name}", "type": connector_type, "topic": "?"})
                except Exception as e:
                    logger.debug("connector %s: %s", name, e)
        # Dedupe by id (same connector can have multiple topics)
        seen = set()
        out = []
        for c in connectors:
            key = (c["id"], c["topic"])
            if key not in seen:
                seen.add(key)
                out.append(c)
        return out

    def _fetch_schemas(self, schema_url: str) -> list[dict[str, Any]]:
        schemas = []
        with httpx.Client(timeout=10.0) as client:
            r = client.get(f"{schema_url}/subjects")
            r.raise_for_status()
            subjects = r.json()
            for subject in subjects:
                try:
                    r2 = client.get(f"{schema_url}/subjects/{subject}/versions/latest")
                    r2.raise_for_status()
                    ver = r2.json()
                    
                    # Extract topic name from subject (e.g., "testtopic-value" -> "testtopic")
                    topic_name = subject.replace("-value", "").replace("-key", "")
                    
                    schemas.append({
                        "subject": subject,
                        "version": ver.get("version", 0),
                        "type": ver.get("schemaType", "AVRO"),
                        "topicName": topic_name,  # Link to topic
                    })
                except Exception as e:
                    logger.debug("subject %s: %s", subject, e)
        return schemas
    
    def fetch_schema_details(self, schema_url: str, subject: str, version: str | None = None) -> dict[str, Any]:
        """
        Fetch full schema details for a specific subject and version.
        If version is None, fetches latest version.
        Also returns list of all available versions.
        Called on-demand when user clicks on a schema node.
        """
        try:
            with httpx.Client(timeout=10.0) as client:
                # First, get all available versions
                versions_response = client.get(f"{schema_url}/subjects/{subject}/versions")
                versions_response.raise_for_status()
                all_versions = versions_response.json()  # Returns list like [1, 2, 3]
                
                # Fetch the requested version (or latest)
                version_path = version if version else "latest"
                r = client.get(f"{schema_url}/subjects/{subject}/versions/{version_path}")
                r.raise_for_status()
                data = r.json()
                
                return {
                    "subject": subject,
                    "version": data.get("version", 0),
                    "id": data.get("id"),
                    "schema": data.get("schema"),  # Full schema content
                    "schemaType": data.get("schemaType", "AVRO"),
                    "allVersions": all_versions,  # List of all available versions
                }
        except Exception as e:
            logger.error(f"Failed to fetch schema for {subject}: {e}")
            raise RuntimeError(f"Schema not found: {subject}") from e
    
    def fetch_topic_details(self, bootstrap_servers: str, topic_name: str, include_messages: bool = False) -> dict[str, Any]:
        """
        Fetch detailed configuration and recent messages for a specific topic.
        Returns: topic config + last 5 messages
        """
        try:
            bootstrap_list = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
            admin = AdminClient({"bootstrap.servers": ",".join(bootstrap_list)})
            
            logger.info(f"Fetching details for topic: {topic_name}")
            
            # Get topic configuration
            from confluent_kafka.admin import ConfigResource, ResourceType
            config_resource = ConfigResource(ResourceType.TOPIC, topic_name)
            configs = admin.describe_configs([config_resource], request_timeout=10)
            
            topic_config = {}
            for res, future in configs.items():
                try:
                    config_result = future.result()
                    topic_config = {
                        entry.name: entry.value 
                        for entry in config_result.values() 
                        if entry.value is not None
                    }
                except Exception as e:
                    logger.warning(f"Could not fetch config for {topic_name}: {e}")
            
            # Get topic metadata (partitions, replication)
            metadata = admin.list_topics(timeout=10)
            topic_metadata = metadata.topics.get(topic_name)
            
            partitions_count = 0
            replication_factor = 0
            if topic_metadata and topic_metadata.partitions:
                partitions_count = len(topic_metadata.partitions)
                # Get replication factor from first partition
                for partition in topic_metadata.partitions.values():
                    if hasattr(partition, 'replicas') and partition.replicas:
                        replication_factor = len(partition.replicas)
                        break
            
            # Extract key configurations
            retention_ms = topic_config.get('retention.ms', 'N/A')
            retention_bytes = topic_config.get('retention.bytes', 'N/A')
            cleanup_policy = topic_config.get('cleanup.policy', 'delete')
            max_message_bytes = topic_config.get('max.message.bytes', 'N/A')
            
            # Convert retention.ms to human-readable format
            if retention_ms != 'N/A':
                try:
                    ms = int(retention_ms)
                    if ms == -1:
                        retention_ms_display = 'Unlimited'
                    else:
                        # Convert to days/hours
                        days = ms // (1000 * 60 * 60 * 24)
                        hours = (ms % (1000 * 60 * 60 * 24)) // (1000 * 60 * 60)
                        if days > 0:
                            retention_ms_display = f"{days}d {hours}h"
                        else:
                            retention_ms_display = f"{hours}h"
                except:
                    retention_ms_display = retention_ms
            else:
                retention_ms_display = 'N/A'
            
            # Fetch recent messages (last 5) - only if requested
            messages = []
            if include_messages:
                try:
                    temp_consumer = Consumer({
                        'bootstrap.servers': ",".join(bootstrap_list),
                        'group.id': f'streamlens-viewer-{os.getpid()}',
                        'enable.auto.commit': False,
                        'auto.offset.reset': 'latest',  # Start from end
                    })
                    
                    # Get all partitions for this topic
                    partitions = [TopicPartition(topic_name, p) for p in range(partitions_count)]
                    
                    # Get high water marks (end offsets)
                    for tp in partitions:
                        low, high = temp_consumer.get_watermark_offsets(tp, cached=False, timeout=2.0)
                        # Seek to 5 messages before the end (or beginning if less than 5)
                        start_offset = max(0, high - 5)
                        tp.offset = start_offset
                    
                    # Assign partitions
                    temp_consumer.assign(partitions)
                    
                    # Consume up to 5 messages total across all partitions
                    timeout = 3  # 3 second timeout
                    start_time = time.time()
                    
                    while len(messages) < 5 and (time.time() - start_time) < timeout:
                        msg = temp_consumer.poll(timeout=0.5)
                        if msg is None:
                            continue
                        if msg.error():
                            continue
                        
                        try:
                            key_str = msg.key().decode('utf-8') if msg.key() else None
                        except:
                            key_str = str(msg.key()) if msg.key() else None
                        
                        try:
                            value_str = msg.value().decode('utf-8') if msg.value() else None
                        except:
                            value_str = '<binary data>'
                        
                        messages.append({
                            'partition': msg.partition(),
                            'offset': msg.offset(),
                            'timestamp': msg.timestamp()[1] if msg.timestamp()[0] else None,
                            'key': key_str,
                            'value': value_str,
                        })
                    
                    temp_consumer.close()
                    
                except Exception as e:
                    logger.warning(f"Could not fetch messages for {topic_name}: {e}")
            
            return {
                'name': topic_name,
                'partitions': partitions_count,
                'replicationFactor': replication_factor,
                'config': {
                    'retentionMs': retention_ms,
                    'retentionMsDisplay': retention_ms_display,
                    'retentionBytes': retention_bytes,
                    'cleanupPolicy': cleanup_policy,
                    'maxMessageBytes': max_message_bytes,
                },
                'recentMessages': messages,
            }
            
        except Exception as e:
            logger.error(f"Failed to fetch topic details for {topic_name}: {e}", exc_info=True)
            raise RuntimeError(f"Could not fetch topic details: {str(e)}") from e

    def produce_message(
        self, bootstrap_servers: str, topic_name: str, value: str, key: str | None = None
    ) -> dict[str, Any]:
        """
        Produce a single message to a topic. Value and optional key are sent as UTF-8.
        Rejects internal topics (names starting with _, e.g. __consumer_offsets, _schemas).
        Returns: {"ok": True, "partition": int, "offset": int} or raises RuntimeError.
        """
        name = (topic_name or "").strip()
        if not name or name.startswith("_"):
            raise RuntimeError("Cannot produce to internal topics (names starting with _)")
        bootstrap_list = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
        if not bootstrap_list:
            raise RuntimeError("No bootstrap servers configured")
        try:
            producer = Producer({
                "bootstrap.servers": ",".join(bootstrap_list),
                "client.id": "streamlens-ui-producer",
            })
            value_bytes = value.encode("utf-8")
            key_bytes = key.encode("utf-8") if key else None
            delivered = {"partition": None, "offset": None, "err": None}

            def delivery_callback(err, msg):
                if err:
                    delivered["err"] = err
                else:
                    delivered["partition"] = msg.partition()
                    delivered["offset"] = msg.offset()

            producer.produce(
                topic_name,
                value=value_bytes,
                key=key_bytes,
                callback=delivery_callback,
            )
            producer.flush(timeout=10)
            if delivered["err"]:
                raise RuntimeError(str(delivered["err"]))
            return {
                "ok": True,
                "partition": delivered["partition"],
                "offset": delivered["offset"],
            }
        except Exception as e:
            logger.exception("Produce failed for topic %s: %s", topic_name, e)
            raise RuntimeError(f"Produce failed: {str(e)}") from e

    def fetch_consumer_lag(self, bootstrap_servers: str, group_id: str) -> dict[str, Any]:
        """
        Fetch consumer lag per partition for a specific consumer group.
        Returns: {"topics": {"topic_name": {"partitions": [{"partition": 0, "currentOffset": 100, "logEndOffset": 150, "lag": 50}]}}}
        """
        try:
            bootstrap_list = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
            admin = AdminClient({"bootstrap.servers": ",".join(bootstrap_list)})
            
            logger.info(f"Fetching consumer lag for group: {group_id}")
            
            # Get committed offsets for this consumer group
            group_request = ConsumerGroupTopicPartitions(group_id)
            group_metadata = admin.list_consumer_group_offsets([group_request], request_timeout=10)
            result = {"topics": {}}
            
            if not group_metadata:
                logger.warning(f"No metadata found for group {group_id}")
                return result
            
            for group_id_key, future in group_metadata.items():
                try:
                    group_topic_partitions = future.result(timeout=10)
                    # Extract the actual topic partitions list from the ConsumerGroupTopicPartitions object
                    partitions_metadata = group_topic_partitions.topic_partitions
                    
                    partition_count = len(partitions_metadata) if partitions_metadata else 0
                    logger.info(f"Found {partition_count} partitions for group {group_id_key}")
                    
                    if not partitions_metadata:
                        logger.warning(f"Consumer group {group_id} has no committed offsets")
                        return result
                    
                    # Group by topic - for now, just show committed offsets
                    # We'll calculate lag by querying high water marks
                    topics_data = {}
                    
                    # Collect all topic-partitions to query watermarks in batch
                    tp_list = []
                    for tp in partitions_metadata:
                        tp_list.append((tp.topic, tp.partition, tp.offset))
                    
                    # Create a single consumer for batch watermark queries
                    if tp_list:
                        temp_consumer = None
                        try:
                            temp_consumer = Consumer({
                                'bootstrap.servers': ",".join(bootstrap_list),
                                'group.id': f'streamlens-lag-{os.getpid()}',
                                'enable.auto.commit': False,
                                'socket.timeout.ms': 5000,
                                'api.version.request': False,
                            })
                            
                            # Query watermarks
                            for topic, partition, committed_offset in tp_list:
                                try:
                                    # Don't assign, just query watermarks directly
                                    topic_partition = TopicPartition(topic, partition)
                                    low, high = temp_consumer.get_watermark_offsets(topic_partition, cached=False, timeout=2.0)
                                    lag = max(0, high - committed_offset) if committed_offset >= 0 else high
                                    
                                    if topic not in topics_data:
                                        topics_data[topic] = {"partitions": []}
                                    
                                    topics_data[topic]["partitions"].append({
                                        "partition": partition,
                                        "currentOffset": committed_offset,
                                        "logEndOffset": high,
                                        "lag": lag,
                                    })
                                except Exception as wm_err:
                                    logger.warning(f"Watermark query failed for {topic}:{partition}: {wm_err}")
                                    # Fallback: add partition info without lag
                                    if topic not in topics_data:
                                        topics_data[topic] = {"partitions": []}
                                    topics_data[topic]["partitions"].append({
                                        "partition": partition,
                                        "currentOffset": committed_offset,
                                        "logEndOffset": committed_offset if committed_offset >= 0 else 0,
                                        "lag": 0,
                                    })
                        finally:
                            if temp_consumer:
                                temp_consumer.close()
                    
                    result = {"topics": topics_data}
                    logger.info(f"Successfully fetched lag for {len(topics_data)} topics")
                except Exception as e:
                    logger.error(f"Failed to get lag for group {group_id_key}: {e}", exc_info=True)
                    raise
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to fetch consumer lag for {group_id}: {e}", exc_info=True)
            raise RuntimeError(f"Could not fetch consumer lag for {group_id}: {str(e)}") from e

    def _load_streams_config(self) -> list[dict[str, Any]]:
        """
        Load Kafka Streams applications from streams.yaml config file.
        Returns list of streams with: name, consumerGroup, inputTopics, outputTopics
        """
        streams = []
        
        # Look for streams.yaml in the server directory
        config_path = Path(__file__).parent.parent / "streams.yaml"
        
        if not config_path.exists():
            logger.debug("No streams.yaml found, skipping streams configuration")
            return streams
        
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            
            if not config or "streams" not in config:
                logger.warning("streams.yaml exists but has no 'streams' key")
                return streams
            
            for stream_config in config.get("streams", []):
                name = stream_config.get("name")
                consumer_group = stream_config.get("consumerGroup")
                input_topics = stream_config.get("inputTopics", [])
                output_topics = stream_config.get("outputTopics", [])
                
                if not name or not consumer_group:
                    logger.warning(f"Skipping invalid stream config: {stream_config}")
                    continue
                
                streams.append({
                    "id": f"streams:{name}",
                    "label": name,  # Used as label on the edge
                    "name": name,
                    "consumerGroup": consumer_group,
                    "consumesFrom": input_topics,
                    "producesTo": output_topics,
                    "source": "config",
                })
                
                logger.info(f"Loaded streams app '{name}': {input_topics} → {output_topics}")
            
        except Exception as e:
            logger.error(f"Failed to load streams.yaml: {e}")
        
        return streams


kafka_service = KafkaService()
