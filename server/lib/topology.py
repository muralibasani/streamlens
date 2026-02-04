from .kafka import kafka_service


def build_topology(cluster_id: int, cluster: dict, registrations: list[dict] | None = None) -> dict:
    """Build topology graph from real cluster state and optional app registrations."""
    try:
        state = kafka_service.fetch_system_state(cluster)
    except Exception:
        # Kafka unreachable or any error: still build from registrations (streams, producers, consumers)
        state = kafka_service._empty_state()
    
    # Merge manual registrations (user-provided metadata)
    # Auto-discovered consumers are already in state["consumers"] from fetch_system_state
    # Auto-discovered producers (from ACLs) are already in state["producers"]
    for reg in registrations or []:
        app_name = reg.get("appName") or reg.get("app_name", "")
        role = (reg.get("role") or "producer").lower()
        topics = reg.get("topics") or []
        output_topics = reg.get("outputTopics") or reg.get("output_topics") or []
        if not app_name:
            continue
        node_id = f"app:{app_name}"
        if role == "streams":
            in_list = list(topics) if topics else []
            out_list = list(output_topics) if output_topics else []
            if not in_list and not out_list:
                continue
            state["streams"].append({
                "id": node_id,
                "label": app_name,
                "consumesFrom": in_list,
                "producesTo": out_list,
                "source": "manual",  # Manual registration
            })
        elif role == "producer":
            if not topics:
                continue
            state["producers"].append({
                "id": node_id,
                "producesTo": topics,
                "source": "manual",  # Manual registration
            })
        else:  # consumer
            if not topics:
                continue
            state["consumers"].append({
                "id": node_id,
                "consumesFrom": topics,
                "source": "manual",  # Manual registration
            })
    nodes = []
    edges = []

    # Topic nodes from cluster
    topic_names = {t["name"] for t in state["topics"]}
    for t in state["topics"]:
        nodes.append({"id": f"topic:{t['name']}", "type": "topic", "data": {"label": t["name"], "details": t}})

    # Ensure topic nodes exist for topics referenced by streams, producers, and consumers
    # This handles cases where topics appear in registrations/JMX/ACLs but not in broker metadata
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
            nodes.append({"id": c["id"], "type": "connector", "data": {"label": c["id"], "type": c["type"]}})
        topic = c.get("topic") or "?"
        if topic != "?":
            if c["type"] == "sink":
                edges.append({"id": f"{topic}->{c['id']}", "source": f"topic:{topic}", "target": c["id"], "type": "sinks", "animated": True})
            else:
                edges.append({"id": f"{c['id']}->{topic}", "source": c["id"], "target": f"topic:{topic}", "type": "sources", "animated": True})

    for s in state["schemas"]:
        topic_name = s["subject"].replace("-value", "").replace("-key", "")
        if any(n["id"] == f"topic:{topic_name}" for n in nodes):
            schema_id = f"schema:{s['subject']}"
            nodes.append({"id": schema_id, "type": "schema", "data": {"label": f"{s['subject']} (v{s['version']})"}})
            edges.append({"id": f"{topic_name}->{schema_id}", "source": f"topic:{topic_name}", "target": schema_id, "type": "has_schema", "style": {"strokeDasharray": "5,5"}})

    return {"nodes": nodes, "edges": edges}
