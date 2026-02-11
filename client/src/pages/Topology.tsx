import { useEffect, useState, useCallback, useMemo, useRef } from "react";
import { useRoute, useLocation } from "wouter";
import ReactFlow, {
  Background,
  Controls,
  useNodesState,
  useEdgesState,
  MarkerType,
  ReactFlowProvider,
  useReactFlow,
} from "reactflow";
import dagre from "dagre";
import { useTopology, useRefreshTopology, useLoadMoreTopics, useCluster, useClusterHealth, TOPICS_PER_PAGE, searchTopologyOnServer } from "@/hooks/use-kafka";
import TopologyNode from "@/components/TopologyNode";
import { StreamsEdge } from "@/components/StreamsEdge";
import { AiChatPanel } from "@/components/AiChatPanel";
import { ThemeToggle } from "@/components/ThemeToggle";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Loader2, RefreshCw, LayoutTemplate, ArrowLeft, Info, Sparkles, Shield, Zap, Search, X, ChevronDown, ChevronUp, CheckCircle2, XCircle, Server, User, Activity, Box, GitBranch, FileJson, AlertTriangle, ArrowRightLeft, MoreHorizontal } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { useTheme } from "@/hooks/use-theme";
import { Link } from "wouter";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";

const NODE_WIDTH = 220;
const NODE_HEIGHT = 100;
const GRID_COLS = 8;
const MAX_NODES_FOR_DAGRE = 1500;

function getGridLayoutedNodes(nodes: any[]): any[] {
  return nodes.map((node, i) => {
    const col = i % GRID_COLS;
    const row = Math.floor(i / GRID_COLS);
    return {
      ...node,
      position: { x: col * (NODE_WIDTH + 40), y: row * (NODE_HEIGHT + 24) },
    };
  });
}

// Helper for auto-layout using Dagre (or grid fallback for very large graphs)
const getLayoutedElements = (nodes: any[], edges: any[], direction = "LR") => {
  if (nodes.length > MAX_NODES_FOR_DAGRE) {
    return {
      nodes: getGridLayoutedNodes(nodes),
      edges,
    };
  }
  const g = new dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: direction });

  edges.forEach((edge) => g.setEdge(edge.source, edge.target));
  nodes.forEach((node) => {
    g.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
  });

  dagre.layout(g);

  return {
    nodes: nodes.map((node) => {
      const pos = g.node(node.id);
      return { ...node, position: { x: pos.x, y: pos.y } };
    }),
    edges,
  };
};

const nodeTypes = {
  kafkaNode: TopologyNode,
};

const edgeTypes = {
  streams: StreamsEdge,
};

// Inner component that uses ReactFlow hooks
function TopologyContent({ clusterId }: { clusterId: number }) {
  const { data: snapshot, isLoading, refetch } = useTopology(clusterId);
  const { data: cluster } = useCluster(clusterId);
  const { data: health } = useClusterHealth(clusterId);
  const refreshTopology = useRefreshTopology();
  const loadMoreMutation = useLoadMoreTopics();
  const { toast } = useToast();
  const { theme } = useTheme();
  const reactFlowInstance = useReactFlow();
  const [, setLocation] = useLocation();

  const backgroundGridColor = theme === "dark" ? "#4b5563" : "#94a3b8";

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [matchingNodes, setMatchingNodes] = useState<string[]>([]);
  const [currentMatchIndex, setCurrentMatchIndex] = useState(0);

  // Pagination state
  const [topicOffset, setTopicOffset] = useState(TOPICS_PER_PAGE);
  const [topicMeta, setTopicMeta] = useState<{
    totalTopicCount: number;
    loadedTopicCount: number;
    hasMore: boolean;
  } | null>(null);
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  // Track the snapshot id that was used for the initial load to detect refreshes
  const initialSnapshotIdRef = useRef<number | null>(null);
  // Refs for latest nodes/edges — used by debounced search so it always reads fresh state
  const nodesRef = useRef(nodes);
  nodesRef.current = nodes;
  const edgesRef = useRef(edges);
  edgesRef.current = edges;

  // Redirect to homepage if cluster is offline
  useEffect(() => {
    if (health && health.online === false) {
      toast({
        title: "Cluster Offline",
        description: "Cannot access topology - cluster is unreachable. Redirecting to home...",
        variant: "destructive",
      });
      setTimeout(() => {
        setLocation("/");
      }, 2000); // Give user time to see the message
    }
  }, [health, setLocation, toast]);

  // Calculate entity counts
  const entityCounts = useMemo(() => {
    const topics = nodes.filter(n => n.data?.type === 'topic').length;
    const schemas = nodes.filter(n => n.data?.type === 'schema').length;
    const producers = nodes.filter(n => n.data?.type === 'producer' || n.id?.startsWith('jmx:')).length;
    const consumers = nodes.filter(n => n.data?.type === 'consumer' || n.id?.startsWith('group:')).length;
    const streams = nodes.filter(n => n.data?.type === 'streams').length;
    const connectors = nodes.filter(n => n.data?.type === 'connector' || n.id?.startsWith('connect:')).length;
    const acls = nodes.filter(n => n.data?.type === 'acl' || n.id?.startsWith('acl:topic:')).length;
    
    return { topics, schemas, producers, consumers, streams, connectors, acls };
  }, [nodes]);

  // Transform snapshot data into ReactFlow elements (initial page)
  useEffect(() => {
    const data = snapshot?.data;
    if (!data || typeof data !== "object") return;

    // Detect snapshot change (refresh): reset pagination
    const sid = (snapshot as any)?.id ?? null;
    if (initialSnapshotIdRef.current !== null && initialSnapshotIdRef.current !== sid) {
      // Snapshot was rebuilt — reset pagination
      setTopicOffset(TOPICS_PER_PAGE);
    }
    initialSnapshotIdRef.current = sid;

    const rawNodes = Array.isArray((data as any).nodes) ? (data as any).nodes : [];
    const rawEdges = Array.isArray((data as any).edges) ? (data as any).edges : [];
    const enableProduceFromUi = cluster?.enableKafkaEventProduceFromUi ?? false;

    const topicIdsWithConnector = new Set<string>();
    for (const e of rawEdges) {
      const src = String(e.source ?? "");
      const tgt = String(e.target ?? "");
      if (src.startsWith("connect:") && tgt.startsWith("topic:")) topicIdsWithConnector.add(tgt);
      if (tgt.startsWith("connect:") && src.startsWith("topic:")) topicIdsWithConnector.add(src);
    }

    const initialNodes = rawNodes.map((n: any) => ({
      ...n,
      type: "kafkaNode",
      data: {
        ...n.data,
        type: n.type,
        highlighted: false,
        searchHighlighted: false,
        ...(n.type === "topic" && {
          enableProduceFromUi,
          hasConnector: topicIdsWithConnector.has(String(n.id)),
        }),
      },
    }));

    const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
      initialNodes,
      rawEdges
    );

    setNodes(layoutedNodes);
    setEdges(
      layoutedEdges.map((e: any) => {
        const isStreams =
          e.type === "streams" || (typeof e.id === "string" && String(e.id).startsWith("streams:"));
        return {
          id: String(e.id),
          source: String(e.source),
          target: String(e.target),
          type: isStreams ? "streams" : "default",
          ...(e.label != null && { label: String(e.label) }),
          markerEnd: { type: MarkerType.ArrowClosed },
          style: isStreams ? undefined : { strokeWidth: 2, ...(e.style || {}) },
          animated: !isStreams && e.animated !== false,
        };
      })
    );

    // Track pagination meta from first page
    const meta = (data as any)?._meta;
    if (meta && typeof meta.totalTopicCount === "number") {
      setTopicMeta({
        totalTopicCount: meta.totalTopicCount,
        loadedTopicCount: meta.loadedTopicCount ?? meta.shownTopicCount ?? rawNodes.filter((n: any) => n.type === "topic").length,
        hasMore: meta.hasMore ?? false,
      });
      setTopicOffset((meta.offset ?? 0) + (meta.limit ?? TOPICS_PER_PAGE));
    } else {
      // No meta = all topics fit on one page
      setTopicMeta(null);
    }
  }, [snapshot, cluster]);

  // Handle manual refresh request
  const handleRefresh = async () => {
    try {
      await refreshTopology.mutateAsync(clusterId);
      // Reset pagination — the invalidation in the mutation will trigger refetch of page 1
      setTopicOffset(TOPICS_PER_PAGE);
      setTopicMeta(null);
      toast({ title: "Topology Refreshed", description: "Latest cluster state loaded." });
    } catch (err) {
      toast({ 
        title: "Refresh Failed", 
        description: "Could not connect to cluster.", 
        variant: "destructive" 
      });
    }
  };

  // Load more topics incrementally
  const handleLoadMore = useCallback(async () => {
    if (!topicMeta?.hasMore || isLoadingMore) return;
    setIsLoadingMore(true);
    try {
      const result = await loadMoreMutation.mutateAsync({
        clusterId,
        offset: topicOffset,
        limit: TOPICS_PER_PAGE,
      });
      const newData = (result as any)?.data;
      if (!newData) return;

      const newRawNodes: any[] = newData.nodes || [];
      const newRawEdges: any[] = newData.edges || [];
      const enableProduceFromUi = cluster?.enableKafkaEventProduceFromUi ?? false;

      // Build connector set from new edges
      const newConnectorTopics = new Set<string>();
      for (const e of newRawEdges) {
        const src = String(e.source ?? "");
        const tgt = String(e.target ?? "");
        if (src.startsWith("connect:") && tgt.startsWith("topic:")) newConnectorTopics.add(tgt);
        if (tgt.startsWith("connect:") && src.startsWith("topic:")) newConnectorTopics.add(src);
      }

      // Deduplicate against existing nodes/edges
      const existingNodeIds = new Set(nodes.map((n) => n.id));
      const existingEdgeIds = new Set(edges.map((e) => String(e.id)));

      // Transform new nodes into ReactFlow format (position is temporary — relayout below)
      const addedNodes: any[] = [];
      for (const n of newRawNodes) {
        if (existingNodeIds.has(n.id)) continue;
        addedNodes.push({
          ...n,
          type: "kafkaNode",
          data: {
            ...n.data,
            type: n.type,
            highlighted: false,
            searchHighlighted: false,
            ...(n.type === "topic" && {
              enableProduceFromUi,
              hasConnector: newConnectorTopics.has(String(n.id)),
            }),
          },
          position: { x: 0, y: 0 }, // placeholder — Dagre will set the real position
        });
      }

      const addedEdges: any[] = [];
      for (const e of newRawEdges) {
        if (existingEdgeIds.has(String(e.id))) continue;
        const isStreams =
          e.type === "streams" || (typeof e.id === "string" && String(e.id).startsWith("streams:"));
        addedEdges.push({
          id: String(e.id),
          source: String(e.source),
          target: String(e.target),
          type: isStreams ? "streams" : "default",
          ...(e.label != null && { label: String(e.label) }),
          markerEnd: { type: MarkerType.ArrowClosed },
          style: isStreams ? undefined : { strokeWidth: 2, ...(e.style || {}) },
          animated: !isStreams && e.animated !== false,
        });
      }

      // Merge and re-run Dagre layout on the entire graph so edges route cleanly
      const mergedNodes = [...nodes, ...addedNodes];
      const mergedEdges = [...edges, ...addedEdges];
      const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
        mergedNodes,
        mergedEdges,
      );
      setNodes(layoutedNodes);
      setEdges(layoutedEdges);

      // Update pagination meta
      const meta = newData._meta;
      if (meta) {
        setTopicMeta({
          totalTopicCount: meta.totalTopicCount,
          loadedTopicCount: meta.loadedTopicCount ?? topicOffset + TOPICS_PER_PAGE,
          hasMore: meta.hasMore ?? false,
        });
        setTopicOffset((meta.offset ?? topicOffset) + (meta.limit ?? TOPICS_PER_PAGE));
      }

      const newTopicCount = addedNodes.filter((n) => n.data?.type === "topic").length;
      if (newTopicCount > 0) {
        toast({
          title: "Topics Loaded",
          description: `Loaded ${newTopicCount} more topics.`,
        });
      }
    } catch (err) {
      toast({
        title: "Load More Failed",
        description: "Could not load additional topics.",
        variant: "destructive",
      });
    } finally {
      setIsLoadingMore(false);
    }
  }, [topicMeta, topicOffset, isLoadingMore, loadMoreMutation, clusterId, cluster, nodes, edges, setNodes, setEdges, toast]);

  // Highlight logic triggered by AI
  const highlightNodes = useCallback(async (nodeIds: string[]) => {
    const currentNodes = nodesRef.current;
    const loadedIds = new Set(currentNodes.map((n) => n.id));
    const missingIds = nodeIds.filter((id) => !loadedIds.has(id));

    // If some requested nodes aren't loaded yet, fetch them from the server
    if (missingIds.length > 0) {
      try {
        // Build search queries from missing IDs (e.g. "topic:orders" -> "orders")
        const queries = missingIds.map((id) => {
          const parts = id.split(":");
          return parts.length > 1 ? parts.slice(1).join(":") : id;
        });

        // Fetch all missing nodes via server search
        const results = await Promise.all(
          queries.map((q) => searchTopologyOnServer(clusterId, q))
        );

        const enableProduce = cluster?.enableKafkaEventProduceFromUi ?? false;
        const latestNodes = nodesRef.current;
        const latestEdges = edgesRef.current;
        const existingNodeIds = new Set(latestNodes.map((n) => n.id));
        const existingEdgeIds = new Set(latestEdges.map((e) => String(e.id)));

        const addedNodesMap = new Map<string, any>();
        const addedEdgesMap = new Map<string, any>();

        for (const result of results) {
          for (const n of (result.nodes || [])) {
            if (!existingNodeIds.has(n.id) && !addedNodesMap.has(n.id)) {
              // Build connector set from edges for this result
              const connectorTopics = new Set<string>();
              for (const e of (result.edges || [])) {
                const src = String(e.source ?? "");
                const tgt = String(e.target ?? "");
                if (src.startsWith("connect:") && tgt.startsWith("topic:")) connectorTopics.add(tgt);
                if (tgt.startsWith("connect:") && src.startsWith("topic:")) connectorTopics.add(src);
              }
              addedNodesMap.set(n.id, {
                ...n,
                type: "kafkaNode",
                data: {
                  ...n.data,
                  type: n.type,
                  highlighted: nodeIds.includes(n.id),
                  searchHighlighted: false,
                  ...(n.type === "topic" && {
                    enableProduceFromUi: enableProduce,
                    hasConnector: connectorTopics.has(String(n.id)),
                  }),
                },
                position: { x: 0, y: 0 },
              });
            }
          }
          for (const e of (result.edges || [])) {
            const eid = String(e.id);
            if (!existingEdgeIds.has(eid) && !addedEdgesMap.has(eid)) {
              const isStreams = e.type === "streams" || eid.startsWith("streams:");
              addedEdgesMap.set(eid, {
                id: eid,
                source: String(e.source),
                target: String(e.target),
                type: isStreams ? "streams" : "default",
                ...(e.label != null && { label: String(e.label) }),
                markerEnd: { type: MarkerType.ArrowClosed },
                style: isStreams ? undefined : { strokeWidth: 2, ...(e.style || {}) },
                animated: !isStreams && e.animated !== false,
              });
            }
          }
        }

        if (addedNodesMap.size > 0 || addedEdgesMap.size > 0) {
          // Merge and re-layout with Dagre
          const mergedNodes = [
            ...latestNodes.map((node) => ({
              ...node,
              data: { ...node.data, highlighted: nodeIds.includes(node.id) },
            })),
            ...Array.from(addedNodesMap.values()),
          ];
          const mergedEdges = [...latestEdges, ...Array.from(addedEdgesMap.values())];

          const { nodes: layoutedNodes, edges: layoutedEdges } =
            getLayoutedElements(mergedNodes, mergedEdges);
          setNodes(layoutedNodes);
          setEdges(layoutedEdges);

          // Zoom after a short delay so React can render the new nodes
          setTimeout(() => {
            const freshNodes = nodesRef.current;
            const highlighted = freshNodes.filter((n) => nodeIds.includes(n.id));
            if (highlighted.length === 1) {
              reactFlowInstance.setCenter(
                highlighted[0].position.x + 110,
                highlighted[0].position.y + 50,
                { zoom: 1.2, duration: 800 }
              );
            } else if (highlighted.length > 1) {
              reactFlowInstance.fitView({
                nodes: highlighted.map((n) => ({ id: n.id })),
                padding: 0.3,
                duration: 800,
                maxZoom: 1.5,
              });
            }
          }, 150);
          return;
        }
      } catch {
        // Server fetch failed — fall through to highlight only loaded nodes
      }
    }

    // All requested nodes are already loaded (or fetch failed) — highlight in place
    setNodes((nds) =>
      nds.map((node) => ({
        ...node,
        data: {
          ...node.data,
          highlighted: nodeIds.includes(node.id),
        },
      }))
    );

    if (nodeIds.length > 0) {
      const highlightedNodes = currentNodes.filter((n) => nodeIds.includes(n.id));

      if (highlightedNodes.length === 1) {
        const node = highlightedNodes[0];
        reactFlowInstance.setCenter(
          node.position.x + 110,
          node.position.y + 50,
          { zoom: 1.2, duration: 800 }
        );
      } else if (highlightedNodes.length > 1) {
        reactFlowInstance.fitView({
          nodes: highlightedNodes.map((n) => ({ id: n.id })),
          padding: 0.3,
          duration: 800,
          maxZoom: 1.5,
        });
      }
    }
  }, [setNodes, setEdges, reactFlowInstance, clusterId, cluster]);

  // Fallback: search the full cluster by user's free-text question (used by AI panel)
  const searchAndNavigate = useCallback(async (query: string) => {
    if (!query.trim()) return;
    try {
      const result = await searchTopologyOnServer(clusterId, query.trim());
      const matchIds: string[] = result.matchIds || [];
      if (matchIds.length === 0) return;

      const serverNodes: any[] = result.nodes || [];
      const serverEdges: any[] = result.edges || [];
      const enableProduce = cluster?.enableKafkaEventProduceFromUi ?? false;

      const currentNodes = nodesRef.current;
      const currentEdges = edgesRef.current;
      const existingNodeIds = new Set(currentNodes.map((n) => n.id));
      const existingEdgeIds = new Set(currentEdges.map((e) => String(e.id)));

      // Build connector set from edges
      const connectorTopics = new Set<string>();
      for (const e of serverEdges) {
        const src = String(e.source ?? "");
        const tgt = String(e.target ?? "");
        if (src.startsWith("connect:") && tgt.startsWith("topic:")) connectorTopics.add(tgt);
        if (tgt.startsWith("connect:") && src.startsWith("topic:")) connectorTopics.add(src);
      }

      const addedNodes = serverNodes
        .filter((n: any) => !existingNodeIds.has(n.id))
        .map((n: any) => ({
          ...n,
          type: "kafkaNode",
          data: {
            ...n.data,
            type: n.type,
            highlighted: matchIds.includes(n.id),
            searchHighlighted: false,
            ...(n.type === "topic" && {
              enableProduceFromUi: enableProduce,
              hasConnector: connectorTopics.has(String(n.id)),
            }),
          },
          position: { x: 0, y: 0 },
        }));

      const addedEdges = serverEdges
        .filter((e: any) => !existingEdgeIds.has(String(e.id)))
        .map((e: any) => {
          const isStreams = e.type === "streams" || String(e.id).startsWith("streams:");
          return {
            id: String(e.id),
            source: String(e.source),
            target: String(e.target),
            type: isStreams ? "streams" : "default",
            ...(e.label != null && { label: String(e.label) }),
            markerEnd: { type: MarkerType.ArrowClosed },
            style: isStreams ? undefined : { strokeWidth: 2, ...(e.style || {}) },
            animated: !isStreams && e.animated !== false,
          };
        });

      // Merge + Dagre re-layout
      const mergedNodes = [
        ...currentNodes.map((node) => ({
          ...node,
          data: { ...node.data, highlighted: matchIds.includes(node.id) },
        })),
        ...addedNodes,
      ];
      const mergedEdges = [...currentEdges, ...addedEdges];

      const { nodes: layoutedNodes, edges: layoutedEdges } =
        getLayoutedElements(mergedNodes, mergedEdges);
      setNodes(layoutedNodes);
      setEdges(layoutedEdges);

      // Zoom to first match after render
      setTimeout(() => {
        const fresh = nodesRef.current;
        const highlighted = fresh.filter((n) => matchIds.includes(n.id));
        if (highlighted.length === 1) {
          reactFlowInstance.setCenter(
            highlighted[0].position.x + 110,
            highlighted[0].position.y + 50,
            { zoom: 1.2, duration: 800 }
          );
        } else if (highlighted.length > 1) {
          reactFlowInstance.fitView({
            nodes: highlighted.map((n) => ({ id: n.id })),
            padding: 0.3,
            duration: 800,
            maxZoom: 1.5,
          });
        }
      }, 150);
    } catch {
      // Server search failed silently
    }
  }, [clusterId, cluster, setNodes, setEdges, reactFlowInstance]);

  // Server-side search state
  const [isSearchingServer, setIsSearchingServer] = useState(false);
  const searchDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Zoom to a specific match by index
  const zoomToMatch = useCallback((index: number) => {
    if (matchingNodes.length === 0) return;
    
    const matchId = matchingNodes[index];
    const matchNode = nodes.find((n) => n.id === matchId);
    
    if (matchNode) {
      reactFlowInstance.setCenter(
        matchNode.position.x + 110,
        matchNode.position.y + 50,
        { zoom: 1.2, duration: 800 }
      );
    }
  }, [matchingNodes, nodes, reactFlowInstance]);

  // Search: local first, then server (debounced) for unloaded topics
  const handleSearch = useCallback((query: string) => {
    setSearchQuery(query);
    setCurrentMatchIndex(0);

    // Cancel any pending server search
    if (searchDebounceRef.current) {
      clearTimeout(searchDebounceRef.current);
      searchDebounceRef.current = null;
    }

    if (!query.trim()) {
      setMatchingNodes([]);
      setIsSearchingServer(false);
      setNodes((nds) =>
        nds.map((node) => ({
          ...node,
          data: { ...node.data, searchHighlighted: false },
        }))
      );
      return;
    }

    // 1. Immediate local search across already-loaded nodes
    const searchLower = query.toLowerCase();
    const localMatches = nodes.filter((node) => {
      const label = node.data?.label?.toLowerCase() || "";
      const type = node.data?.type?.toLowerCase() || "";
      const id = node.id.toLowerCase();
      return label.includes(searchLower) || type.includes(searchLower) || id.includes(searchLower);
    });

    const localMatchIds = localMatches.map((n) => n.id);
    setMatchingNodes(localMatchIds);

    setNodes((nds) =>
      nds.map((node) => ({
        ...node,
        data: {
          ...node.data,
          searchHighlighted: localMatchIds.includes(node.id),
        },
      }))
    );

    if (localMatches.length > 0) {
      const firstMatch = localMatches[0];
      reactFlowInstance.setCenter(
        firstMatch.position.x + 110,
        firstMatch.position.y + 50,
        { zoom: 1.2, duration: 800 }
      );
    }

    // 2. If there are unloaded topics, debounce a server search to find matches
    //    in the full snapshot and merge any new nodes into the graph.
    if (topicMeta?.hasMore && query.trim().length >= 2) {
      setIsSearchingServer(true);
      const enableProduce = cluster?.enableKafkaEventProduceFromUi ?? false;

      searchDebounceRef.current = setTimeout(async () => {
        try {
          const result = await searchTopologyOnServer(clusterId, query.trim());
          const serverMatchIds: string[] = result.matchIds || [];
          if (serverMatchIds.length === 0) {
            setIsSearchingServer(false);
            return;
          }

          const serverNodes: any[] = result.nodes || [];
          const serverEdges: any[] = result.edges || [];

          // Build connector set from server edges
          const connectorTopics = new Set<string>();
          for (const e of serverEdges) {
            const src = String(e.source ?? "");
            const tgt = String(e.target ?? "");
            if (src.startsWith("connect:") && tgt.startsWith("topic:")) connectorTopics.add(tgt);
            if (tgt.startsWith("connect:") && src.startsWith("topic:")) connectorTopics.add(src);
          }

          const allMatchIdSet = new Set([...localMatchIds, ...serverMatchIds]);
          const hadNoLocalMatches = localMatchIds.length === 0;

          // Read latest nodes/edges from refs (fresh state at execution time)
          const currentNodes = nodesRef.current;
          const currentEdges = edgesRef.current;

          const existingNodeIds = new Set(currentNodes.map((n) => n.id));
          const existingEdgeIds = new Set(currentEdges.map((e) => String(e.id)));

          // Build new ReactFlow nodes from server results
          const addedNodes = serverNodes
            .filter((n: any) => !existingNodeIds.has(n.id))
            .map((n: any) => ({
              ...n,
              type: "kafkaNode",
              data: {
                ...n.data,
                type: n.type,
                highlighted: false,
                searchHighlighted: allMatchIdSet.has(n.id),
                ...(n.type === "topic" && {
                  enableProduceFromUi: enableProduce,
                  hasConnector: connectorTopics.has(String(n.id)),
                }),
              },
              position: { x: 0, y: 0 }, // placeholder — Dagre will set the real position
            }));

          // Build new ReactFlow edges from server results
          const addedEdges = serverEdges
            .filter((e: any) => !existingEdgeIds.has(String(e.id)))
            .map((e: any) => {
              const isStreams =
                e.type === "streams" || String(e.id).startsWith("streams:");
              return {
                id: String(e.id),
                source: String(e.source),
                target: String(e.target),
                type: isStreams ? "streams" : "default",
                ...(e.label != null && { label: String(e.label) }),
                markerEnd: { type: MarkerType.ArrowClosed },
                style: isStreams ? undefined : { strokeWidth: 2, ...(e.style || {}) },
                animated: !isStreams && e.animated !== false,
              };
            });

          // Merge all nodes (update search highlighting on existing ones) + run Dagre
          const mergedNodes = [
            ...currentNodes.map((node) => ({
              ...node,
              data: { ...node.data, searchHighlighted: allMatchIdSet.has(node.id) },
            })),
            ...addedNodes,
          ];
          const mergedEdges = [...currentEdges, ...addedEdges];

          const { nodes: layoutedNodes, edges: layoutedEdges } =
            getLayoutedElements(mergedNodes, mergedEdges);
          setNodes(layoutedNodes);
          setEdges(layoutedEdges);

          // Update match list
          const allMatchIds = Array.from(allMatchIdSet);
          setMatchingNodes(allMatchIds);

          // If local search had no hits but server found some, zoom to the first
          if (hadNoLocalMatches && allMatchIds.length > 0) {
            setTimeout(() => {
              const first = nodesRef.current.find((n) => allMatchIdSet.has(n.id));
              if (first) {
                reactFlowInstance.setCenter(
                  first.position.x + 110,
                  first.position.y + 50,
                  { zoom: 1.2, duration: 800 }
                );
              }
            }, 150);
          }
        } catch {
          // Server search failed silently — local results still shown
        } finally {
          setIsSearchingServer(false);
        }
      }, 350);
    }
  }, [nodes, setNodes, setEdges, reactFlowInstance, topicMeta, clusterId, cluster]);

  // Navigate to next match
  const nextMatch = useCallback(() => {
    if (matchingNodes.length === 0) return;
    const nextIndex = (currentMatchIndex + 1) % matchingNodes.length;
    setCurrentMatchIndex(nextIndex);
    zoomToMatch(nextIndex);
  }, [currentMatchIndex, matchingNodes.length, zoomToMatch]);

  // Navigate to previous match
  const prevMatch = useCallback(() => {
    if (matchingNodes.length === 0) return;
    const prevIndex = (currentMatchIndex - 1 + matchingNodes.length) % matchingNodes.length;
    setCurrentMatchIndex(prevIndex);
    zoomToMatch(prevIndex);
  }, [currentMatchIndex, matchingNodes.length, zoomToMatch]);

  // Clear search
  const clearSearch = useCallback(() => {
    if (searchDebounceRef.current) {
      clearTimeout(searchDebounceRef.current);
      searchDebounceRef.current = null;
    }
    setSearchQuery("");
    setMatchingNodes([]);
    setCurrentMatchIndex(0);
    setIsSearchingServer(false);
    setNodes((nds) =>
      nds.map((node) => ({
        ...node,
        data: { ...node.data, searchHighlighted: false },
      }))
    );
  }, [setNodes]);

  if (isLoading) {
    return (
      <div className="h-screen flex items-center justify-center bg-background text-primary">
        <Loader2 className="w-10 h-10 animate-spin" />
      </div>
    );
  }

  if (!snapshot && !isLoading) {
    return (
      <div className="h-screen flex flex-col items-center justify-center bg-background gap-4">
        <h2 className="text-2xl font-bold">No Topology Data Yet</h2>
        <Button onClick={handleRefresh} disabled={refreshTopology.isPending}>
          {refreshTopology.isPending ? "Crawling..." : "Run Initial Crawl"}
        </Button>
      </div>
    );
  }

  return (
    <div className="h-screen w-screen flex flex-col bg-background">
      {/* Header Toolbar */}
      <div className="h-20 border-b border-border bg-card flex items-center justify-between px-6 shrink-0 z-10">
        <div className="flex items-center gap-4">
          <Link href="/">
            <Button variant="ghost" size="icon" className="rounded-full">
              <ArrowLeft className="w-5 h-5" />
            </Button>
          </Link>
          <div className="flex items-center gap-3 pr-4 border-r border-border">
            <img 
              src="/streamlens-logo.svg" 
              alt="streamLens" 
              className="w-8 h-8 object-contain"
            />
            <span className="font-black text-2xl tracking-tight bg-gradient-to-r from-primary to-primary/60 bg-clip-text text-transparent">
              streamLens
            </span>
          </div>
          <div className="flex flex-col">
            <div className="flex items-center gap-2">
              <h1 className="font-bold text-lg tracking-tight">{cluster?.name || "Loading..."}</h1>
              {health?.online ? (
                <div className="flex items-center gap-1 text-xs text-green-700 dark:text-green-400 bg-green-100 dark:bg-green-950/30 px-2 py-0.5 rounded border border-green-300 dark:border-green-900/50">
                  <CheckCircle2 className="w-3 h-3" />
                  <span>Online</span>
                </div>
              ) : (
                <div className="flex items-center gap-1 text-xs text-red-700 dark:text-red-400 bg-red-100 dark:bg-red-950/30 px-2 py-0.5 rounded border border-red-300 dark:border-red-900/50">
                  <XCircle className="w-3 h-3" />
                  <span>Offline</span>
                </div>
              )}
            </div>
            <div className="flex items-center gap-3 text-[10px] text-muted-foreground font-mono opacity-70 flex-wrap">
              <span className="flex items-center gap-1">
                <Server className="w-3 h-3" />
                {cluster?.bootstrapServers || "—"}
              </span>
              {cluster?.schemaRegistryUrl && (
                <>
                  <span>•</span>
                  <span className="flex items-center gap-1 text-blue-600 dark:text-blue-400">
                    <span>Schema:</span>
                    <span>{cluster.schemaRegistryUrl}</span>
                  </span>
                </>
              )}
              {cluster?.connectUrl && (
                <>
                  <span>•</span>
                  <span className="flex items-center gap-1 text-purple-600 dark:text-purple-400">
                    <span>Connect:</span>
                    <span>{cluster.connectUrl}</span>
                  </span>
                </>
              )}
              <span>•</span>
              <span>ID: {clusterId}</span>
            </div>
          </div>
        </div>
        
        <div className="flex items-center gap-2">
          <Popover>
            <PopoverTrigger asChild>
              <Button variant="ghost" size="icon" className="rounded-full">
                <Info className="w-4 h-4" />
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-80" align="end">
              <div className="space-y-3">
                <h3 className="font-semibold text-sm">Entity Sources</h3>
                <div className="space-y-2 text-sm">
                  <div className="flex items-start gap-2">
                    <div className="flex items-center gap-1 text-[10px] text-green-700 dark:text-green-400 bg-green-100 dark:bg-green-950/30 px-1.5 py-0.5 rounded border border-green-300 dark:border-green-900/50 whitespace-nowrap mt-0.5">
                      <Sparkles className="w-3 h-3" />
                      <span>Live</span>
                    </div>
                    <p className="text-muted-foreground text-xs leading-relaxed">
                      Auto-discovered from Kafka (consumer groups). Updated in real-time, no client changes needed.
                    </p>
                  </div>
                  <div className="flex items-start gap-2">
                    <div className="flex items-center gap-1 text-[10px] text-yellow-700 dark:text-yellow-400 bg-yellow-100 dark:bg-yellow-950/30 px-1.5 py-0.5 rounded border border-yellow-300 dark:border-yellow-900/50 whitespace-nowrap mt-0.5">
                      <Zap className="w-3 h-3" />
                      <span>JMX</span>
                    </div>
                    <p className="text-muted-foreground text-xs leading-relaxed">
                      Active producers detected from JMX metrics. Shows topics receiving messages <i>right now</i>. Requires JMX enabled on brokers.
                    </p>
                  </div>
                </div>
              </div>
            </PopoverContent>
          </Popover>
          
          {/* Search Input */}
          <div className="relative w-64">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-muted-foreground" />
            <Input
              type="text"
              placeholder="Search nodes..."
              value={searchQuery}
              onChange={(e) => handleSearch(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                  if (e.shiftKey) {
                    prevMatch();
                  } else {
                    nextMatch();
                  }
                }
              }}
              className="pl-10 pr-8 h-9 bg-background/50 border-border"
            />
            {searchQuery && matchingNodes.length > 1 && (
              <div className="absolute right-8 top-1/2 transform -translate-y-1/2 flex gap-0.5">
                <button
                  onClick={prevMatch}
                  className="text-muted-foreground hover:text-foreground p-0.5 rounded hover:bg-muted"
                  title="Previous match (Shift+Enter)"
                >
                  <ChevronUp className="w-3 h-3" />
                </button>
                <button
                  onClick={nextMatch}
                  className="text-muted-foreground hover:text-foreground p-0.5 rounded hover:bg-muted"
                  title="Next match (Enter)"
                >
                  <ChevronDown className="w-3 h-3" />
                </button>
              </div>
            )}
            {searchQuery && (
              <button
                onClick={clearSearch}
                className="absolute right-2 top-1/2 transform -translate-y-1/2 text-muted-foreground hover:text-foreground"
              >
                <X className="w-4 h-4" />
              </button>
            )}
            {searchQuery && (
              <div className="absolute -bottom-5 left-0 text-xs text-muted-foreground whitespace-nowrap">
                {isSearchingServer ? (
                  <span className="flex items-center gap-1">
                    <Loader2 className="w-3 h-3 animate-spin" />
                    {matchingNodes.length > 0
                      ? `${currentMatchIndex + 1} of ${matchingNodes.length}+ — searching all topics…`
                      : "Searching all topics…"}
                  </span>
                ) : matchingNodes.length > 0 ? (
                  <>
                    {currentMatchIndex + 1} of {matchingNodes.length} match{matchingNodes.length !== 1 ? 'es' : ''}
                  </>
                ) : (
                  'No matches'
                )}
              </div>
            )}
          </div>
          
          <Button 
            variant="outline" 
            size="sm" 
            onClick={() => {
              const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(nodes, edges);
              setNodes([...layoutedNodes]);
              setEdges([...layoutedEdges]);
            }}
          >
            <LayoutTemplate className="w-4 h-4 mr-2" />
            Auto Layout
          </Button>
          <Button 
            onClick={handleRefresh} 
            disabled={refreshTopology.isPending} 
            className="bg-primary text-primary-foreground hover:bg-primary/90"
          >
            <RefreshCw className={`w-4 h-4 mr-2 ${refreshTopology.isPending ? 'animate-spin' : ''}`} />
            Sync
          </Button>
          <ThemeToggle />
        </div>
      </div>

      {/* Offline Warning Banner */}
      {health && !health.online && (
        <div className="bg-yellow-100 dark:bg-yellow-950/30 border-y border-yellow-300 dark:border-yellow-900/50 px-6 py-3 flex items-center gap-3">
          <AlertTriangle className="w-5 h-5 text-yellow-600 dark:text-yellow-400 flex-shrink-0" />
          <div className="flex-1">
            <p className="text-sm font-semibold text-yellow-700 dark:text-yellow-400">Cluster Offline</p>
            <p className="text-xs text-yellow-600/80 dark:text-yellow-400/80">
              {health.error || "Cannot connect to Kafka cluster. Showing cached topology data."}
            </p>
          </div>
          <Button 
            variant="outline" 
            size="sm"
            onClick={handleRefresh}
            disabled={refreshTopology.isPending}
            className="border-yellow-300 dark:border-yellow-900/50 hover:bg-yellow-200 dark:hover:bg-yellow-950/50"
          >
            <RefreshCw className={`w-4 h-4 mr-2 ${refreshTopology.isPending ? 'animate-spin' : ''}`} />
            Retry Connection
          </Button>
        </div>
      )}


      {/* Main Content Area */}
      <div className="flex-1 flex overflow-hidden">
        {/* Graph Area */}
        <div className="flex-1 relative bg-neutral-100 dark:bg-neutral-900/50">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            fitView
            className="bg-dots-pattern"
          >
            <Background
              color={backgroundGridColor}
              gap={20}
              size={1}
            />
            <Controls className="!bg-card !border-border !fill-foreground" />
          </ReactFlow>

          {/* Stats Panel - Floating in top-right corner */}
          <div className="absolute top-4 right-4 z-10 bg-card/95 backdrop-blur-sm border border-border rounded-lg shadow-xl p-4 min-w-[220px]">
            <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
              Cluster Overview
            </h3>
            <div className="space-y-2">
              <div className="flex items-center justify-between text-sm">
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 rounded-full bg-[hsl(var(--node-topic))]" />
                  <span className="text-muted-foreground">Topics</span>
                </div>
                <span className="font-bold text-foreground">{entityCounts.topics}</span>
              </div>
              <div className="flex items-center justify-between text-sm">
                <div className="flex items-center gap-2">
                  <FileJson className="w-3 h-3 text-[hsl(var(--node-schema))]" />
                  <span className="text-muted-foreground">Schemas</span>
                </div>
                <span className="font-bold text-foreground">{entityCounts.schemas}</span>
              </div>
              <div className="flex items-center justify-between text-sm">
                <div className="flex items-center gap-2">
                  <Box className="w-3 h-3 text-[hsl(var(--node-producer))]" />
                  <span className="text-muted-foreground">Producers</span>
                </div>
                <span className="font-bold text-foreground">{entityCounts.producers}</span>
              </div>
              <div className="flex items-center justify-between text-sm">
                <div className="flex items-center gap-2">
                  <Activity className="w-3 h-3 text-[hsl(var(--node-consumer))]" />
                  <span className="text-muted-foreground">Consumer Groups</span>
                </div>
                <span className="font-bold text-foreground">{entityCounts.consumers}</span>
              </div>
              <div className="flex items-center justify-between text-sm">
                <div className="flex items-center gap-2">
                  <Shield className="w-3 h-3 text-[hsl(var(--node-acl))]" />
                  <span className="text-muted-foreground">ACLs</span>
                </div>
                <span className="font-bold text-foreground">{entityCounts.acls}</span>
              </div>
              {entityCounts.streams > 0 && (
                <div className="flex items-center justify-between text-sm">
                  <div className="flex items-center gap-2">
                    <GitBranch className="w-3 h-3 text-[hsl(var(--node-streams))]" />
                    <span className="text-muted-foreground">Streams Apps</span>
                  </div>
                  <span className="font-bold text-foreground">{entityCounts.streams}</span>
                </div>
              )}
              {entityCounts.connectors > 0 && (
                <div className="flex items-center justify-between text-sm">
                  <div className="flex items-center gap-2">
                    <ArrowRightLeft className="w-3 h-3 text-[hsl(var(--node-connector))]" />
                    <span className="text-muted-foreground">Connectors</span>
                  </div>
                  <span className="font-bold text-foreground">{entityCounts.connectors}</span>
                </div>
              )}
            </div>

          </div>

          {/* Load More Topics - Floating panel in bottom-right corner */}
          {topicMeta?.hasMore && (
            <div className="absolute bottom-4 right-4 z-10 bg-card/95 backdrop-blur-sm border border-border rounded-lg shadow-xl px-4 py-3 flex items-center gap-3">
              <span className="text-xs text-muted-foreground whitespace-nowrap">
                {Math.min(topicMeta.loadedTopicCount, topicMeta.totalTopicCount).toLocaleString()} of{" "}
                {topicMeta.totalTopicCount.toLocaleString()} topics
              </span>
              <Button
                variant="outline"
                size="sm"
                onClick={handleLoadMore}
                disabled={isLoadingMore}
                className="h-7 text-xs px-3"
              >
                {isLoadingMore ? (
                  <>
                    <Loader2 className="w-3 h-3 mr-1.5 animate-spin" />
                    Loading...
                  </>
                ) : (
                  <>
                    <MoreHorizontal className="w-3 h-3 mr-1.5" />
                    Load More
                  </>
                )}
              </Button>
            </div>
          )}
        </div>

        {/* Right Sidebar - AI Chat */}
        <div className="w-[400px] shrink-0 border-l border-border bg-card">
          <AiChatPanel 
            topology={snapshot} 
            onHighlightNodes={highlightNodes}
            onSearchAndNavigate={searchAndNavigate}
          />
        </div>
      </div>
    </div>
  );
}

// Main component wrapper with ReactFlowProvider
export default function Topology() {
  const [, params] = useRoute("/topology/:id");
  const clusterId = Number(params?.id);

  return (
    <ReactFlowProvider>
      <TopologyContent clusterId={clusterId} />
    </ReactFlowProvider>
  );
}
