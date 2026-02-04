import { useEffect, useState, useCallback, useMemo, useRef } from "react";
import { useRoute } from "wouter";
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
import { useTopology, useRefreshTopology, useCluster } from "@/hooks/use-kafka";
import TopologyNode from "@/components/TopologyNode";
import { StreamsEdge } from "@/components/StreamsEdge";
import { AiChatPanel } from "@/components/AiChatPanel";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Loader2, RefreshCw, LayoutTemplate, ArrowLeft, Info, Sparkles, Shield, Zap, Search, X, ChevronDown, ChevronUp, CheckCircle2, XCircle, Server, User, Activity, Box, GitBranch, FileJson } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { Link } from "wouter";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";

// Helper for auto-layout using Dagre
const getLayoutedElements = (nodes: any[], edges: any[], direction = "LR") => {
  const g = new dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: direction });

  edges.forEach((edge) => g.setEdge(edge.source, edge.target));
  nodes.forEach((node) => {
    // Width/Height needs to match the Node component dimensions roughly
    g.setNode(node.id, { width: 220, height: 100 });
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
  const refreshTopology = useRefreshTopology();
  const { toast } = useToast();
  const reactFlowInstance = useReactFlow();

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [matchingNodes, setMatchingNodes] = useState<string[]>([]);
  const [currentMatchIndex, setCurrentMatchIndex] = useState(0);

  // Calculate entity counts
  const entityCounts = useMemo(() => {
    const topics = nodes.filter(n => n.data?.type === 'topic').length;
    const schemas = nodes.filter(n => n.data?.type === 'schema').length;
    const producers = nodes.filter(n => n.data?.type === 'producer' || n.id?.startsWith('jmx:')).length;
    const consumers = nodes.filter(n => n.data?.type === 'consumer' || n.id?.startsWith('group:')).length;
    const streams = nodes.filter(n => n.data?.type === 'streams').length;
    
    return { topics, schemas, producers, consumers, streams };
  }, [nodes]);

  // Transform snapshot data into ReactFlow elements
  useEffect(() => {
    const data = snapshot?.data;
    if (!data || typeof data !== "object") return;

    const rawNodes = Array.isArray((data as any).nodes) ? (data as any).nodes : [];
    const rawEdges = Array.isArray((data as any).edges) ? (data as any).edges : [];

    const initialNodes = rawNodes.map((n: any) => ({
      ...n,
      type: "kafkaNode",
      data: { ...n.data, type: n.type, highlighted: false, searchHighlighted: false },
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
  }, [snapshot]);

  // Handle manual refresh request
  const handleRefresh = async () => {
    try {
      await refreshTopology.mutateAsync(clusterId);
      toast({ title: "Topology Refreshed", description: "Latest cluster state loaded." });
      refetch();
    } catch (err) {
      toast({ 
        title: "Refresh Failed", 
        description: "Could not connect to cluster.", 
        variant: "destructive" 
      });
    }
  };

  // Highlight logic triggered by AI
  const highlightNodes = useCallback((nodeIds: string[]) => {
    setNodes((nds) =>
      nds.map((node) => ({
        ...node,
        data: {
          ...node.data,
          highlighted: nodeIds.includes(node.id),
        },
      }))
    );

    // Zoom to highlighted nodes
    if (nodeIds.length > 0) {
      const highlightedNodes = nodes.filter((n) => nodeIds.includes(n.id));
      
      if (highlightedNodes.length === 1) {
        // Single node: zoom to it
        const node = highlightedNodes[0];
        reactFlowInstance.setCenter(
          node.position.x + 110,
          node.position.y + 50,
          { zoom: 1.2, duration: 800 }
        );
      } else if (highlightedNodes.length > 1) {
        // Multiple nodes: fit all of them in view
        const nodeIds = highlightedNodes.map((n) => n.id);
        reactFlowInstance.fitView({
          nodes: nodeIds.map((id) => ({ id })),
          padding: 0.3,
          duration: 800,
          maxZoom: 1.5,
        });
      }
    }
  }, [setNodes, nodes, reactFlowInstance]);

  // Zoom to a specific match by index
  const zoomToMatch = useCallback((index: number) => {
    if (matchingNodes.length === 0) return;
    
    const matchId = matchingNodes[index];
    const matchNode = nodes.find((n) => n.id === matchId);
    
    if (matchNode) {
      reactFlowInstance.setCenter(
        matchNode.position.x + 110, // offset to center (node width/2)
        matchNode.position.y + 50,  // offset to center (node height/2)
        { zoom: 1.2, duration: 800 }
      );
    }
  }, [matchingNodes, nodes, reactFlowInstance]);

  // Search and zoom to matching nodes
  const handleSearch = useCallback((query: string) => {
    setSearchQuery(query);
    setCurrentMatchIndex(0);
    
    if (!query.trim()) {
      setMatchingNodes([]);
      // Remove search highlighting
      setNodes((nds) =>
        nds.map((node) => ({
          ...node,
          data: {
            ...node.data,
            searchHighlighted: false,
          },
        }))
      );
      return;
    }

    const searchLower = query.toLowerCase();
    const matches = nodes.filter((node) => {
      const label = node.data?.label?.toLowerCase() || "";
      const type = node.data?.type?.toLowerCase() || "";
      const id = node.id.toLowerCase();
      return label.includes(searchLower) || type.includes(searchLower) || id.includes(searchLower);
    });

    const matchIds = matches.map((n) => n.id);
    setMatchingNodes(matchIds);

    // Highlight matching nodes
    setNodes((nds) =>
      nds.map((node) => ({
        ...node,
        data: {
          ...node.data,
          searchHighlighted: matchIds.includes(node.id),
        },
      }))
    );

    // Zoom to first match if found
    if (matches.length > 0) {
      const firstMatch = matches[0];
      reactFlowInstance.setCenter(
        firstMatch.position.x + 110, // offset to center (node width/2)
        firstMatch.position.y + 50,  // offset to center (node height/2)
        { zoom: 1.2, duration: 800 }
      );
    }
  }, [nodes, setNodes, reactFlowInstance]);

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
    setSearchQuery("");
    setMatchingNodes([]);
    setCurrentMatchIndex(0);
    setNodes((nds) =>
      nds.map((node) => ({
        ...node,
        data: {
          ...node.data,
          searchHighlighted: false,
        },
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
              src="https://kafka.apache.org/logos/kafka_logo--simple.png" 
              alt="Kafka Logo" 
              className="w-8 h-8 object-contain"
            />
            <span className="font-black text-2xl tracking-tight bg-gradient-to-r from-primary to-primary/60 bg-clip-text text-transparent">
              StreamLens
            </span>
          </div>
          <div className="flex flex-col">
            <div className="flex items-center gap-2">
              <h1 className="font-bold text-lg tracking-tight">{cluster?.name || "Loading..."}</h1>
              {snapshot ? (
                <div className="flex items-center gap-1 text-xs text-green-400 bg-green-950/30 px-2 py-0.5 rounded border border-green-900/50">
                  <CheckCircle2 className="w-3 h-3" />
                  <span>Connected</span>
                </div>
              ) : (
                <div className="flex items-center gap-1 text-xs text-red-400 bg-red-950/30 px-2 py-0.5 rounded border border-red-900/50">
                  <XCircle className="w-3 h-3" />
                  <span>Disconnected</span>
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
                  <span className="flex items-center gap-1 text-blue-400">
                    <span>Schema:</span>
                    <span>{cluster.schemaRegistryUrl}</span>
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
                    <div className="flex items-center gap-1 text-[10px] text-green-400 bg-green-950/30 px-1.5 py-0.5 rounded border border-green-900/50 whitespace-nowrap mt-0.5">
                      <Sparkles className="w-3 h-3" />
                      <span>Live</span>
                    </div>
                    <p className="text-muted-foreground text-xs leading-relaxed">
                      Auto-discovered from Kafka (consumer groups). Updated in real-time, no client changes needed.
                    </p>
                  </div>
                  <div className="flex items-start gap-2">
                    <div className="flex items-center gap-1 text-[10px] text-yellow-400 bg-yellow-950/30 px-1.5 py-0.5 rounded border border-yellow-900/50 whitespace-nowrap mt-0.5">
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
              <div className="absolute -bottom-5 left-0 text-xs text-muted-foreground">
                {matchingNodes.length > 0 ? (
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
        </div>
      </div>

      {/* Main Content Area */}
      <div className="flex-1 flex overflow-hidden">
        {/* Graph Area */}
        <div className="flex-1 relative bg-neutral-900/50">
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
            <Background color="#333" gap={20} size={1} />
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
              {entityCounts.streams > 0 && (
                <div className="flex items-center justify-between text-sm">
                  <div className="flex items-center gap-2">
                    <GitBranch className="w-3 h-3 text-[hsl(var(--node-streams))]" />
                    <span className="text-muted-foreground">Streams Apps</span>
                  </div>
                  <span className="font-bold text-foreground">{entityCounts.streams}</span>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Right Sidebar - AI Chat */}
        <div className="w-[400px] shrink-0 border-l border-border bg-card">
          <AiChatPanel 
            topology={snapshot} 
            onHighlightNodes={highlightNodes} 
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
