// Compute a filtered subgraph: matched nodes + one-hop neighbors + connecting edges
export function computeFilteredGraph(
  allNodes: any[],
  allEdges: any[],
  query: string
): { nodes: any[]; edges: any[]; matchIds: string[] } {
  if (!query.trim()) {
    return { nodes: allNodes, edges: allEdges, matchIds: [] };
  }
  const searchLower = query.trim().toLowerCase();
  const matchIds: string[] = [];
  const matchIdSet = new Set<string>();

  for (const node of allNodes) {
    const label = node.data?.label?.toLowerCase() || "";
    const type = node.data?.type?.toLowerCase() || "";
    const id = node.id.toLowerCase();
    if (label.includes(searchLower) || type.includes(searchLower) || id.includes(searchLower)) {
      matchIds.push(node.id);
      matchIdSet.add(node.id);
    }
  }

  if (matchIds.length === 0) {
    return { nodes: [], edges: [], matchIds: [] };
  }

  // Find one-hop neighbors via edges
  const visibleIds = new Set(matchIdSet);
  for (const edge of allEdges) {
    const src = String(edge.source);
    const tgt = String(edge.target);
    if (matchIdSet.has(src)) visibleIds.add(tgt);
    if (matchIdSet.has(tgt)) visibleIds.add(src);
  }

  const filteredNodes = allNodes
    .filter((n) => visibleIds.has(n.id))
    .map((n) => ({
      ...n,
      data: { ...n.data, searchHighlighted: matchIdSet.has(n.id) },
    }));

  const filteredEdges = allEdges.filter(
    (e) => visibleIds.has(String(e.source)) && visibleIds.has(String(e.target))
  );

  return { nodes: filteredNodes, edges: filteredEdges, matchIds };
}
