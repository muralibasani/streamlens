from .kafka import kafka_service


def build_topology(cluster_id: int, cluster: dict) -> dict:
    """Build topology graph from real cluster state."""
    try:
        state = kafka_service.fetch_system_state(cluster)
    except Exception:
        state = kafka_service._empty_state()

    nodes = []
    edges = []

    # Topic nodes from cluster
    topic_names = {t["name"] for t in state["topics"]}
    for t in state["topics"]:
        topic_data = {
            "label": t["name"],
            "details": t
        }
        nodes.append({"id": f"topic:{t['name']}", "type": "topic", "data": topic_data})

    # Ensure topic nodes exist for topics referenced by streams, producers, and consumers
    # This handles cases where topics appear in JMX/ACLs/connectors but not in broker metadata
    for s in state["streams"]:
        for name in s["consumesFrom"] + s["producesTo"]:
            if name not in topic_names:
                topic_names.add(name)
                nodes.append({"id": f"topic:{name}", "type": "topic", "data": {"label": name, "details": None}})
    
    for p in state["producers"]:
        for name in p.get("producesTo", []):
            if name not in topic_names:
                topic_names.add(name)
                nodes.append({"id": f"topic:{name}", "type": "topic", "data": {"label": name, "details": None}})
    
    for c in state["consumers"]:
        for name in c.get("consumesFrom", []):
            if name not in topic_names:
                topic_names.add(name)
                nodes.append({"id": f"topic:{name}", "type": "topic", "data": {"label": name, "details": None}})

    # Ensure topic nodes exist for topics that have ACLs
    for a in state.get("acls", []):
        name = a.get("topic")
        if name and name not in topic_names:
            topic_names.add(name)
            nodes.append({"id": f"topic:{name}", "type": "topic", "data": {"label": name, "details": None}})

    for p in state["producers"]:
        source = p.get("source", "unknown")
        label = p.get("label") or p["id"]
        
        # Clean up label for display
        if label.startswith("app:"):
            label = label.replace("app:", "", 1)
        elif label.startswith("acl:"):
            label = label.replace("acl:", "", 1)
        elif label.startswith("jmx:active-producer:"):
            # For JMX producers, use a cleaner label
            label = label.replace("jmx:active-producer:", "Active → ")
        
        nodes.append({
            "id": p["id"],
            "type": "producer",
            "data": {
                "label": label,
                "source": source,
                "principal": p.get("principal"),  # For ACL-based producers
            }
        })
        for topic in p["producesTo"]:
            # Different edge styles for different sources
            if source == "acl":
                edge_style = {"strokeDasharray": "5,5"}  # Dashed for potential (ACL)
                animated = False
            elif source == "jmx":
                edge_style = {"strokeDasharray": "2,2"}  # Fine dashed for JMX (active)
                animated = True
            else:
                edge_style = {}  # Solid for manual
                animated = True
            
            edges.append({
                "id": f"{p['id']}->{topic}",
                "source": p["id"],
                "target": f"topic:{topic}",
                "type": "produces",
                "animated": animated,
                "style": edge_style,
            })

    for c in state["consumers"]:
        source = c.get("source", "unknown")
        label = c["id"]
        # Clean up label for display
        if label.startswith("app:"):
            label = label.replace("app:", "", 1)
        elif label.startswith("group:"):
            label = label.replace("group:", "", 1)
        
        nodes.append({
            "id": c["id"],
            "type": "consumer",
            "data": {
                "label": label,
                "source": source,
            }
        })
        for topic in c["consumesFrom"]:
            edges.append({
                "id": f"{topic}->{c['id']}",
                "source": f"topic:{topic}",
                "target": c["id"],
                "type": "consumes",
                "animated": True,
            })

    # Streams: direct edges from input topic to output topic (no middle node), with app name on the edge
    for s in state["streams"]:
        app_label = s.get("label", s["id"].replace("app:", "", 1))
        consumes = s.get("consumesFrom") or []
        produces = s.get("producesTo") or []
        for in_topic in consumes:
            for out_topic in produces:
                edge_id = f"streams:{s['id']}:{in_topic}:{out_topic}"
                edges.append({
                    "id": edge_id,
                    "source": f"topic:{in_topic}",
                    "target": f"topic:{out_topic}",
                    "type": "streams",
                    "label": app_label,
                    "animated": True,
                    "style": {"strokeDasharray": "8,4"},
                })

    # Connectors: one node per connector id (dedupe), one edge per topic
    connector_nodes_added = set()
    for c in state["connectors"]:
        if c["id"] not in connector_nodes_added:
            connector_nodes_added.add(c["id"])
            # Clean up label: remove "connect:" prefix for display
            connector_label = c["id"].replace("connect:", "", 1) if c["id"].startswith("connect:") else c["id"]
            nodes.append({"id": c["id"], "type": "connector", "data": {"label": connector_label, "type": c["type"]}})
        topic = c.get("topic") or "?"
        if topic != "?":
            if c["type"] == "sink":
                edges.append({"id": f"{topic}->{c['id']}", "source": f"topic:{topic}", "target": c["id"], "type": "sinks", "animated": True})
            else:
                edges.append({"id": f"{c['id']}->{topic}", "source": c["id"], "target": f"topic:{topic}", "type": "sources", "animated": True})

    # ACL nodes: one node per topic (grouped), edge from ACL node to topic; click shows list of bindings
    topic_acls: dict[str, list[dict[str, Any]]] = {}
    for a in state.get("acls", []):
        topic = a.get("topic")
        if not topic:
            continue
        if topic not in topic_acls:
            topic_acls[topic] = []
        topic_acls[topic].append({
            "principal": a.get("principal", "") or "",
            "host": a.get("host", "") or "",
            "operation": a.get("operation", "") or "?",
            "permissionType": a.get("permissionType", "") or "?",
        })
    for topic, acl_list in topic_acls.items():
        if not acl_list:
            continue
        acl_id = f"acl:topic:{topic}"
        count = len(acl_list)
        label = f"ACL ({count})" if count > 1 else "ACL"
        nodes.append({
            "id": acl_id,
            "type": "acl",
            "data": {
                "label": label,
                "topic": topic,
                "acls": acl_list,
            },
        })
        edges.append({
            "id": f"{acl_id}->topic:{topic}",
            "source": acl_id,
            "target": f"topic:{topic}",
            "type": "acl",
            "animated": False,
            "style": {"strokeDasharray": "5,5", "stroke": "#b45309"},
        })

    # Create schema nodes and link them to topics
    for s in state["schemas"]:
        topic_name = s.get("topicName")
        if topic_name and any(n["id"] == f"topic:{topic_name}" for n in nodes):
            schema_id = f"schema:{s['subject']}"
            nodes.append({
                "id": schema_id,
                "type": "schema",
                "data": {
                    "label": s["subject"],
                    "subLabel": f"v{s['version']}",
                    "schemaType": s.get("type", "AVRO"),
                    "subject": s["subject"],
                    "version": s["version"]
                }
            })
            # Link schema to topic with a dashed edge
            edges.append({
                "id": f"{topic_name}->{schema_id}",
                "source": f"topic:{topic_name}",
                "target": schema_id,
                "type": "schema_link",
                "style": {"strokeDasharray": "3,3", "stroke": "#60a5fa"}
            })

    return {"nodes": nodes, "edges": edges}
