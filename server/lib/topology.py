import os
import random
from typing import Any

from .kafka import kafka_service

DEFAULT_MAX_TOPICS = 2000
DEFAULT_TOPIC_PAGE_SIZE = 50


def build_topology(cluster_id: int, cluster: dict) -> dict:
    """Build topology graph from real cluster state.
    When the cluster has more than TOPOLOGY_MAX_TOPICS topics, only a subset is included
    (all connected topics + a random sample of the rest) to avoid OOM and browser freezes.
    """
    try:
        state = kafka_service.fetch_system_state(cluster)
    except Exception:
        state = kafka_service._empty_state()

    nodes = []
    edges = []
    all_broker_topics = {t["name"] for t in state["topics"]}
    total_topic_count = len(state["topics"])

    # Topics referenced by producers, consumers, streams, connectors, or ACLs (always include these)
    connected = set()
    for s in state["streams"]:
        connected.update((s.get("consumesFrom") or []) + (s.get("producesTo") or []))
    for p in state["producers"]:
        connected.update(p.get("producesTo") or [])
    for c in state["consumers"]:
        connected.update(c.get("consumesFrom") or [])
    for c in state.get("connectors", []):
        t = c.get("topic")
        if t and t != "?":
            connected.add(t)
    for a in state.get("acls", []):
        t = a.get("topic")
        if t:
            connected.add(t)

    max_topics = int(os.environ.get("TOPOLOGY_MAX_TOPICS", str(DEFAULT_MAX_TOPICS)))
    if max_topics < 1:
        max_topics = DEFAULT_MAX_TOPICS

    if total_topic_count <= max_topics:
        topics_to_show = all_broker_topics
        meta = None
    else:
        connected_in_broker = connected & all_broker_topics
        others = list(all_broker_topics - connected_in_broker)
        sample_size = max(0, max_topics - len(connected_in_broker))
        shown_others = set(random.sample(others, min(sample_size, len(others)))) if others else set()
        topics_to_show = connected_in_broker | shown_others
        meta = {"totalTopicCount": total_topic_count, "shownTopicCount": len(topics_to_show)}

    topic_names = set(topics_to_show)
    for t in state["topics"]:
        name = t["name"]
        if name not in topics_to_show:
            continue
        topic_data = {
            "label": name,
            "details": t,
        }
        nodes.append({"id": f"topic:{name}", "type": "topic", "data": topic_data})

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

    result = {"nodes": nodes, "edges": edges}
    if meta is not None:
        result["_meta"] = meta
    return result


def paginate_topology_data(data: dict, offset: int = 0, limit: int = DEFAULT_TOPIC_PAGE_SIZE) -> dict:
    """
    Return a paginated slice of topic nodes from the full topology snapshot.
    Connected topics (those with producers/consumers/connectors/streams/ACLs) come first,
    then standalone topics alphabetically.  Non-topic nodes are included only when they
    have at least one edge to a visible topic.  Edges are filtered to visible endpoints.
    """
    all_nodes: list[dict] = data.get("nodes") or []
    all_edges: list[dict] = data.get("edges") or []

    # Separate topic nodes from non-topic nodes
    topic_nodes: list[dict] = []
    non_topic_nodes: list[dict] = []
    for n in all_nodes:
        if n.get("type") == "topic":
            topic_nodes.append(n)
        else:
            non_topic_nodes.append(n)

    # Identify topics that have connections (edges to/from non-topic nodes, or streams edges)
    connected_topic_ids: set[str] = set()
    for e in all_edges:
        src = str(e.get("source", ""))
        tgt = str(e.get("target", ""))
        if src.startswith("topic:") and not tgt.startswith("topic:"):
            connected_topic_ids.add(src)
        if tgt.startswith("topic:") and not src.startswith("topic:"):
            connected_topic_ids.add(tgt)
        # Streams: topic-to-topic
        if src.startswith("topic:") and tgt.startswith("topic:"):
            connected_topic_ids.add(src)
            connected_topic_ids.add(tgt)

    # Stable sort: connected first, then alphabetical by label
    def _sort_key(n: dict):
        is_connected = 0 if n["id"] in connected_topic_ids else 1
        label = (n.get("data") or {}).get("label", "")
        return (is_connected, label.lower())

    topic_nodes.sort(key=_sort_key)

    total_topics = len(topic_nodes)
    page_topics = topic_nodes[offset : offset + limit]
    page_topic_ids = {n["id"] for n in page_topics}

    # Build index: non-topic node id → set of connected topic ids (for fast lookup)
    non_topic_to_topics: dict[str, set[str]] = {}
    for e in all_edges:
        src = str(e.get("source", ""))
        tgt = str(e.get("target", ""))
        if src.startswith("topic:") and not tgt.startswith("topic:"):
            non_topic_to_topics.setdefault(tgt, set()).add(src)
        elif tgt.startswith("topic:") and not src.startswith("topic:"):
            non_topic_to_topics.setdefault(src, set()).add(tgt)

    # Include non-topic nodes that have at least one edge to a visible topic
    visible_non_topic: list[dict] = []
    for n in non_topic_nodes:
        linked_topics = non_topic_to_topics.get(n["id"], set())
        if linked_topics & page_topic_ids:
            visible_non_topic.append(n)

    visible_ids = page_topic_ids | {n["id"] for n in visible_non_topic}

    # Include edges where both endpoints are visible
    visible_edges = [
        e for e in all_edges
        if str(e.get("source", "")) in visible_ids and str(e.get("target", "")) in visible_ids
    ]

    return {
        "nodes": page_topics + visible_non_topic,
        "edges": visible_edges,
        "_meta": {
            "totalTopicCount": total_topics,
            "loadedTopicCount": min(offset + limit, total_topics),
            "offset": offset,
            "limit": limit,
            "hasMore": offset + limit < total_topics,
        },
    }


def search_topology(data: dict, query: str) -> dict:
    """
    Search ALL nodes in the full topology snapshot by label, id, or type.
    Returns matching nodes together with their directly-connected non-topic nodes
    and the edges that link them — so the client can merge them into the visible graph.
    Also returns ``matchIds`` (the ids of nodes that matched the query text).
    """
    query_lower = (query or "").lower().strip()
    if not query_lower:
        return {"nodes": [], "edges": [], "matchIds": []}

    all_nodes: list[dict] = data.get("nodes") or []
    all_edges: list[dict] = data.get("edges") or []

    # 1. Find every node whose label / id / type contains the query
    match_ids: set[str] = set()
    for n in all_nodes:
        label = ((n.get("data") or {}).get("label") or "").lower()
        nid = (n.get("id") or "").lower()
        ntype = (n.get("type") or "").lower()
        if query_lower in label or query_lower in nid or query_lower in ntype:
            match_ids.add(n["id"])

    if not match_ids:
        return {"nodes": [], "edges": [], "matchIds": []}

    # 2. For every matching topic node, also include directly-connected non-topic nodes
    matching_topic_ids = {mid for mid in match_ids if mid.startswith("topic:")}
    connected_ids: set[str] = set()
    for e in all_edges:
        src = str(e.get("source", ""))
        tgt = str(e.get("target", ""))
        if src in matching_topic_ids and not tgt.startswith("topic:"):
            connected_ids.add(tgt)
        if tgt in matching_topic_ids and not src.startswith("topic:"):
            connected_ids.add(src)

    visible_ids = match_ids | connected_ids
    visible_nodes = [n for n in all_nodes if n["id"] in visible_ids]
    visible_edges = [
        e for e in all_edges
        if str(e.get("source", "")) in visible_ids and str(e.get("target", "")) in visible_ids
    ]

    return {
        "nodes": visible_nodes,
        "edges": visible_edges,
        "matchIds": sorted(match_ids),
    }
