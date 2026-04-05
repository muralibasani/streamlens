import { describe, it, expect } from "vitest";
import { computeFilteredGraph } from "./filterGraph";

const makeNode = (id: string, label: string, type: string) => ({
  id,
  data: { label, type },
});

const makeEdge = (id: string, source: string, target: string) => ({
  id,
  source,
  target,
});

describe("computeFilteredGraph", () => {
  const nodes = [
    makeNode("topic:orders", "orders", "topic"),
    makeNode("topic:payments", "payments", "topic"),
    makeNode("topic:logs", "logs", "topic"),
    makeNode("group:order-service", "order-service", "consumer"),
    makeNode("producer:payment-svc", "payment-svc", "producer"),
  ];
  const edges = [
    makeEdge("e1", "topic:orders", "group:order-service"),
    makeEdge("e2", "producer:payment-svc", "topic:payments"),
    makeEdge("e3", "topic:payments", "group:order-service"),
  ];

  it("returns matched node + connected neighbors + edges between visible nodes", () => {
    const result = computeFilteredGraph(nodes, edges, "orders");

    // Only topic:orders matches directly (label "orders" contains "orders")
    expect(result.matchIds).toEqual(["topic:orders"]);

    const visibleIds = result.nodes.map((n) => n.id).sort();
    expect(visibleIds).toContain("topic:orders");
    // group:order-service is a one-hop neighbor via e1
    expect(visibleIds).toContain("group:order-service");

    // topic:logs has no connection to any matched node
    expect(visibleIds).not.toContain("topic:logs");

    // Matched nodes should have searchHighlighted = true
    const ordersNode = result.nodes.find((n) => n.id === "topic:orders");
    expect(ordersNode?.data.searchHighlighted).toBe(true);

    // Neighbor (not directly matched) should have searchHighlighted = false
    const consumerNode = result.nodes.find((n) => n.id === "group:order-service");
    expect(consumerNode?.data.searchHighlighted).toBe(false);
  });

  it("returns full graph for empty query", () => {
    const result = computeFilteredGraph(nodes, edges, "");
    expect(result.nodes).toEqual(nodes);
    expect(result.edges).toEqual(edges);
    expect(result.matchIds).toEqual([]);
  });

  it("returns full graph for whitespace-only query", () => {
    const result = computeFilteredGraph(nodes, edges, "   ");
    expect(result.nodes).toEqual(nodes);
    expect(result.edges).toEqual(edges);
    expect(result.matchIds).toEqual([]);
  });

  it("returns empty arrays when no matches found", () => {
    const result = computeFilteredGraph(nodes, edges, "nonexistent");
    expect(result.nodes).toEqual([]);
    expect(result.edges).toEqual([]);
    expect(result.matchIds).toEqual([]);
  });

  it("excludes edges when only one endpoint is visible", () => {
    // Search for "logs" — only topic:logs matches, no edges connect to it
    const result = computeFilteredGraph(nodes, edges, "logs");
    expect(result.matchIds).toEqual(["topic:logs"]);
    expect(result.nodes).toHaveLength(1);
    expect(result.edges).toHaveLength(0);
  });

  it("matches by node type", () => {
    const result = computeFilteredGraph(nodes, edges, "producer");
    expect(result.matchIds).toEqual(["producer:payment-svc"]);
    // producer connects to topic:payments (neighbor via e2)
    expect(result.nodes.map((n) => n.id).sort()).toEqual([
      "producer:payment-svc",
      "topic:payments",
    ]);
  });

  it("matches by node id", () => {
    const result = computeFilteredGraph(nodes, edges, "topic:payments");
    expect(result.matchIds).toContain("topic:payments");
    // Neighbors: producer:payment-svc and group:order-service
    const visibleIds = result.nodes.map((n) => n.id).sort();
    expect(visibleIds).toContain("producer:payment-svc");
    expect(visibleIds).toContain("group:order-service");
  });

  it("is case-insensitive", () => {
    const result = computeFilteredGraph(nodes, edges, "ORDERS");
    expect(result.matchIds).toEqual(["topic:orders"]);
  });
});
