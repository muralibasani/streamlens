import { memo, useState } from 'react';
import { Handle, Position } from 'reactflow';
import { Activity, Box, ArrowRightLeft, FileJson, GitBranch, Send, Sparkles, Shield, User, Zap, Code, Copy } from 'lucide-react';
import { cn } from '@/lib/utils';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useToast } from '@/hooks/use-toast';
import { useRoute } from 'wouter';

// Custom horizontal cylinder icon for Kafka topics
const CylinderIcon = ({ className }: { className?: string }) => (
  <svg
    className={className}
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    {/* Left ellipse (vertical) */}
    <ellipse cx="5" cy="12" rx="2.5" ry="8" />
    {/* Right ellipse (vertical) */}
    <ellipse cx="19" cy="12" rx="2.5" ry="8" />
    {/* Top line connecting ellipses */}
    <line x1="5" y1="4" x2="19" y2="4" />
    {/* Bottom line connecting ellipses */}
    <line x1="5" y1="20" x2="19" y2="20" />
  </svg>
);

// Custom node component for distinct visual styles per Kafka entity type
const NodeIcon = ({ type }: { type: string }) => {
  switch (type) {
    case 'topic': return <CylinderIcon className="w-5 h-5 text-[hsl(var(--node-topic))]" />;
    case 'producer': return <Box className="w-5 h-5 text-[hsl(var(--node-producer))]" />;
    case 'consumer': return <Activity className="w-5 h-5 text-[hsl(var(--node-consumer))]" />;
    case 'streams': return <GitBranch className="w-5 h-5 text-[hsl(var(--node-streams))]" />;
    case 'connector': return <ArrowRightLeft className="w-5 h-5 text-[hsl(var(--node-connector))]" />;
    case 'schema': return <FileJson className="w-5 h-5 text-[hsl(var(--node-schema))]" />;
    case 'acl': return <Shield className="w-5 h-5 text-[hsl(var(--node-acl))]" />;
    default: return <Box className="w-5 h-5 text-muted-foreground" />;
  }
};

const NodeLabel = ({ type }: { type: string }) => {
  switch (type) {
    case 'topic': return <span className="text-[10px] uppercase font-bold text-[hsl(var(--node-topic))] tracking-wider">Topic</span>;
    case 'producer': return <span className="text-[10px] uppercase font-bold text-[hsl(var(--node-producer))] tracking-wider">Producer</span>;
    case 'consumer': return <span className="text-[10px] uppercase font-bold text-[hsl(var(--node-consumer))] tracking-wider">Consumer</span>;
    case 'streams': return <span className="text-[10px] uppercase font-bold text-[hsl(var(--node-streams))] tracking-wider">Streams</span>;
    case 'connector': return <span className="text-[10px] uppercase font-bold text-[hsl(var(--node-connector))] tracking-wider">Connector</span>;
    case 'schema': return <span className="text-[10px] uppercase font-bold text-[hsl(var(--node-schema))] tracking-wider">Schema</span>;
    case 'acl': return <span className="text-[10px] uppercase font-bold text-[hsl(var(--node-acl))] tracking-wider">ACL</span>;
    default: return null;
  }
};

const SourceBadge = ({ source }: { source?: string }) => {
  if (!source) return null;
  
  switch (source) {
    case 'auto-discovered':
      return (
        <div className="flex items-center gap-1 text-[10px] text-green-700 dark:text-green-400 bg-green-100 dark:bg-green-950/30 px-1.5 py-0.5 rounded border border-green-300 dark:border-green-900/50">
          <Sparkles className="w-3 h-3" />
          <span>Live</span>
        </div>
      );
    case 'config':
      return (
        <div className="flex items-center gap-1 text-[10px] text-purple-700 dark:text-purple-400 bg-purple-100 dark:bg-purple-950/30 px-1.5 py-0.5 rounded border border-purple-300 dark:border-purple-900/50">
          <User className="w-3 h-3" />
          <span>Config</span>
        </div>
      );
    case 'jmx':
      return (
        <div className="flex items-center gap-1 text-[10px] text-yellow-700 dark:text-yellow-400 bg-yellow-100 dark:bg-yellow-950/30 px-1.5 py-0.5 rounded border border-yellow-300 dark:border-yellow-900/50">
          <Zap className="w-3 h-3" />
          <span>JMX</span>
        </div>
      );
    case 'offset':
      return (
        <div className="flex items-center gap-1 text-[10px] text-teal-700 dark:text-teal-400 bg-teal-100 dark:bg-teal-950/30 px-1.5 py-0.5 rounded border border-teal-300 dark:border-teal-900/50">
          <Activity className="w-3 h-3" />
          <span>Offset</span>
        </div>
      );
    case 'acl':
      return (
        <div className="flex items-center gap-1 text-[10px] text-amber-700 dark:text-amber-400 bg-amber-100 dark:bg-amber-950/30 px-1.5 py-0.5 rounded border border-amber-300 dark:border-amber-900/50">
          <Shield className="w-3 h-3" />
          <span>ACL</span>
        </div>
      );
    case 'manual':
      return (
        <div className="flex items-center gap-1 text-[10px] text-blue-400 bg-blue-950/30 px-1.5 py-0.5 rounded border border-blue-900/50">
          <User className="w-3 h-3" />
          <span>Manual</span>
        </div>
      );
    default:
      return null;
  }
};

export default memo(({ data, selected }: { data: any, selected: boolean }) => {
  const isHighlighted = data.highlighted || data.searchHighlighted;
  const isSearchMatch = data.searchHighlighted;
  const source = data.source || data.details?.source;
  const [, params] = useRoute("/topology/:id");
  const clusterId = params?.id || "";
  const [showSchemaDialog, setShowSchemaDialog] = useState(false);
  const [schemaDetails, setSchemaDetails] = useState<any>(null);
  const [isLoadingSchema, setIsLoadingSchema] = useState(false);
  const [selectedSchemaVersion, setSelectedSchemaVersion] = useState<number | null>(null);
  
  const [showConsumerLagDialog, setShowConsumerLagDialog] = useState(false);
  const [consumerLag, setConsumerLag] = useState<any>(null);
  const [isLoadingLag, setIsLoadingLag] = useState(false);
  
  const [showTopicDialog, setShowTopicDialog] = useState(false);
  const [topicDetails, setTopicDetails] = useState<any>(null);
  const [produceValue, setProduceValue] = useState("");
  const [produceKey, setProduceKey] = useState("");
  const [isProducing, setIsProducing] = useState(false);

  const [showAclDialog, setShowAclDialog] = useState(false);
  const { toast } = useToast();
  const [isLoadingTopic, setIsLoadingTopic] = useState(false);
  const [isLoadingMessages, setIsLoadingMessages] = useState(false);
  const [messagesLoaded, setMessagesLoaded] = useState(false);

  const [codeGenClient, setCodeGenClient] = useState<'producer' | 'consumer' | 'streams'>('producer');
  const [codeGenLanguage, setCodeGenLanguage] = useState<'java' | 'python'>('java');
  const [codeGenSchemaRegistry, setCodeGenSchemaRegistry] = useState(false);
  const [codeGenOutputTopic, setCodeGenOutputTopic] = useState(''); // for streams: target topic
  const [generatedCode, setGeneratedCode] = useState<string | null>(null);
  const [isLoadingCode, setIsLoadingCode] = useState(false);
  
  const [showConnectorDialog, setShowConnectorDialog] = useState(false);
  const [connectorDetails, setConnectorDetails] = useState<any>(null);
  const [isLoadingConnector, setIsLoadingConnector] = useState(false);

  const fetchSchemaVersion = async (version?: number) => {
    setIsLoadingSchema(true);
    
    try {
      const versionParam = version ? `?version=${version}` : '';
      const res = await fetch(`/api/clusters/${clusterId}/schema/${encodeURIComponent(data.subject)}${versionParam}`);
      if (res.ok) {
        const details = await res.json();
        setSchemaDetails(details);
        setSelectedSchemaVersion(details.version);
      }
    } catch (error) {
      console.error('Failed to fetch schema:', error);
    } finally {
      setIsLoadingSchema(false);
    }
  };

  const handleSchemaClick = async () => {
    if (data.type !== 'schema') return;
    
    setShowSchemaDialog(true);
    setSelectedSchemaVersion(null);
    await fetchSchemaVersion(); // Fetch latest version
  };

  const handleVersionChange = async (version: number) => {
    await fetchSchemaVersion(version);
  };

  const handleConsumerClick = async () => {
    if (data.type !== 'consumer') return;
    
    setShowConsumerLagDialog(true);
    setIsLoadingLag(true);
    
    try {
      // Extract group ID from node ID (format: "group:groupname")
      const groupId = data.label || data.details?.id || '';
      const res = await fetch(`/api/clusters/${clusterId}/consumer/${encodeURIComponent(groupId)}/lag`);
      if (res.ok) {
        const lagData = await res.json();
        setConsumerLag(lagData);
      }
    } catch (error) {
      console.error('Failed to fetch consumer lag:', error);
    } finally {
      setIsLoadingLag(false);
    }
  };

  const handleTopicClick = async () => {
    if (data.type !== 'topic') return;
    
    setShowTopicDialog(true);
    setIsLoadingTopic(true);
    setMessagesLoaded(false);
    setGeneratedCode(null);
    
    try {
      const topicName = data.label || '';
      // Fetch only config, not messages
      const res = await fetch(`/api/clusters/${clusterId}/topic/${encodeURIComponent(topicName)}/details`);
      if (res.ok) {
        const details = await res.json();
        setTopicDetails(details);
      }
    } catch (error) {
      console.error('Failed to fetch topic details:', error);
    } finally {
      setIsLoadingTopic(false);
    }
  };

  const handleGenerateCode = async () => {
    const topicName = topicDetails?.name || data.label || '';
    if (!topicName) return;
    setIsLoadingCode(true);
    setGeneratedCode(null);
    try {
      const params: Record<string, string> = {
        client: codeGenClient,
        language: codeGenClient === 'streams' ? 'java' : codeGenLanguage,
        schema_registry: codeGenClient === 'streams' ? 'false' : String(codeGenSchemaRegistry),
      };
      if (codeGenClient === 'streams' && codeGenOutputTopic.trim()) {
        params.output_topic = codeGenOutputTopic.trim();
      }
      const res = await fetch(`/api/clusters/${clusterId}/topic/${encodeURIComponent(topicName)}/code?${new URLSearchParams(params)}`);
      if (res.ok) {
        const json = await res.json();
        setGeneratedCode(json.code ?? '');
      } else {
        toast({ title: 'Failed to generate code', variant: 'destructive' });
      }
    } catch (e) {
      console.error('Failed to fetch code:', e);
      toast({ title: 'Failed to generate code', variant: 'destructive' });
    } finally {
      setIsLoadingCode(false);
    }
  };

  const handleCopyCode = async () => {
    if (!generatedCode) return;
    try {
      await navigator.clipboard.writeText(generatedCode);
      toast({ title: 'Copied to clipboard' });
    } catch {
      toast({ title: 'Failed to copy', variant: 'destructive' });
    }
  };

  const handleLoadMessages = async () => {
    if (!topicDetails) return;
    
    setIsLoadingMessages(true);
    
    try {
      const topicName = topicDetails.name || data.label || '';
      // Fetch with messages included
      const res = await fetch(`/api/clusters/${clusterId}/topic/${encodeURIComponent(topicName)}/details?include_messages=true`);
      if (res.ok) {
        const details = await res.json();
        setTopicDetails(details);
        setMessagesLoaded(true);
      }
    } catch (error) {
      console.error('Failed to fetch topic messages:', error);
    } finally {
      setIsLoadingMessages(false);
    }
  };

  const topicName = (data.type === "topic" ? (data.label || "") : "") as string;
  const isInternalTopic = topicName.startsWith("_");

  const handleProduce = async () => {
    const value = produceValue.trim();
    if (!value) {
      toast({ title: "Message required", description: "Enter message text to produce.", variant: "destructive" });
      return;
    }
    setIsProducing(true);
    try {
      const res = await fetch(`/api/clusters/${clusterId}/topic/${encodeURIComponent(topicName)}/produce`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value, key: produceKey.trim() || undefined }),
      });
      const result = await res.json().catch(() => ({}));
      if (res.ok && result.ok) {
        toast({
          title: "Message produced",
          description: `Partition ${result.partition}, offset ${result.offset}`,
        });
        setProduceValue("");
        setProduceKey("");
        if (topicDetails && messagesLoaded) {
          handleLoadMessages();
        }
      } else {
        toast({
          title: "Produce failed",
          description: result.detail || res.statusText || "Could not produce message",
          variant: "destructive",
        });
      }
    } catch (error) {
      toast({
        title: "Produce failed",
        description: error instanceof Error ? error.message : "Network error",
        variant: "destructive",
      });
    } finally {
      setIsProducing(false);
    }
  };

  const handleConnectorClick = async () => {
    if (data.type !== 'connector') return;
    
    setShowConnectorDialog(true);
    setIsLoadingConnector(true);
    
    try {
      // Extract connector name from node ID (format: "connect:connector-name")
      const connectorName = data.label || '';
      const res = await fetch(`/api/clusters/${clusterId}/connector/${encodeURIComponent(connectorName)}/details`);
      if (res.ok) {
        const details = await res.json();
        setConnectorDetails(details);
      }
    } catch (error) {
      console.error('Failed to fetch connector details:', error);
    } finally {
      setIsLoadingConnector(false);
    }
  };

  const handleAclClick = () => {
    if (data.type !== 'acl') return;
    setShowAclDialog(true);
  };

  const shapeClass = {
    topic: "rounded-full",
    producer: "rounded-md",
    consumer: "rounded-3xl",
    streams: "rounded-xl",
    connector: "rounded-none node-shape-connector",
    schema: "rounded-tl-2xl rounded-tr-lg rounded-br-xl rounded-bl-lg",
    acl: "rounded-none node-shape-acl",
  }[data.type as keyof typeof shapeClass] ?? "rounded-xl";

  return (
    <>
      <div 
        className={cn(
          "min-w-[180px] px-4 py-3 bg-card border-2 transition-all duration-300 shadow-xl",
          shapeClass,
          selected ? "border-primary ring-4 ring-primary/10" : "border-border",
          isHighlighted ? "border-primary shadow-[0_0_20px_hsl(var(--primary)/0.3)] scale-105" : "",
          isSearchMatch ? "ring-2 ring-yellow-500/50" : "",
          "hover:border-primary/50",
          data.type === 'topic' && "cursor-pointer hover:border-purple-500 hover:shadow-purple-500/20",
          data.type === 'schema' && "cursor-pointer hover:border-blue-500 hover:shadow-blue-500/20",
          data.type === 'consumer' && "cursor-pointer hover:border-green-500 hover:shadow-green-500/20",
          data.type === 'connector' && "cursor-pointer hover:border-orange-500 hover:shadow-orange-500/20",
          data.type === 'acl' && "cursor-pointer hover:border-amber-500/50 hover:shadow-amber-500/20 border-amber-500/40"
        )}
        onClick={
          data.type === 'topic' ? handleTopicClick : 
          data.type === 'schema' ? handleSchemaClick : 
          data.type === 'consumer' ? handleConsumerClick : 
          data.type === 'connector' ? handleConnectorClick :
          data.type === 'acl' ? handleAclClick :
          undefined
        }
      >
        <Handle type="target" position={Position.Left} className="!bg-muted-foreground !w-2 !h-2" />
      
      <div className="flex flex-col gap-1">
        <div className="flex items-center justify-between gap-2 mb-1">
          <NodeLabel type={data.type} />
          <div className="flex items-center gap-1">
            <SourceBadge source={source} />
            {data.metrics && (
               <span className="text-[10px] text-muted-foreground font-mono bg-muted px-1.5 py-0.5 rounded">
                 {data.metrics}
               </span>
            )}
          </div>
        </div>
        
        <div className="flex items-center gap-3">
          <div className={cn(
            "p-2 rounded-lg bg-background border border-border/50",
            isHighlighted && "bg-primary/10 border-primary/20"
          )}>
            <NodeIcon type={data.type} />
          </div>
          <div className="flex flex-col min-w-0">
            <TooltipProvider delayDuration={300}>
              <Tooltip>
                <TooltipTrigger asChild>
                  <span className="font-semibold text-sm truncate max-w-[140px] cursor-default">
                    {data.label}
                  </span>
                </TooltipTrigger>
                <TooltipContent side="top" className="max-w-xs break-words">
                  <p className="font-mono text-xs">{data.label}</p>
                  {data.details && (
                    <p className="text-xs text-muted-foreground mt-1">
                      {data.type === 'topic' && `Partitions: ${data.details.partitions || 'N/A'}`}
                    </p>
                  )}
                  {data.type === 'acl' && data.topic && (
                    <p className="text-xs text-muted-foreground mt-1">
                      Topic: {data.topic}
                      {Array.isArray(data.acls) && data.acls.length > 0 && ` · ${data.acls.length} binding(s)`}
                    </p>
                  )}
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
            {data.subLabel && (
              <TooltipProvider delayDuration={300}>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <span className="text-xs text-muted-foreground truncate max-w-[140px] cursor-default">
                      {data.subLabel}
                    </span>
                  </TooltipTrigger>
                  <TooltipContent side="bottom" className="max-w-xs break-words">
                    <p className="font-mono text-xs">{data.subLabel}</p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            )}
          </div>
        </div>
      </div>

      <Handle type="source" position={Position.Right} className="!bg-muted-foreground !w-2 !h-2" />
    </div>

    {/* Schema Details Dialog */}
    <Dialog open={showSchemaDialog} onOpenChange={setShowSchemaDialog}>
        <DialogContent className="max-w-3xl max-h-[80vh]">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <FileJson className="w-5 h-5 text-blue-500" />
              Schema: {data.subject}
            </DialogTitle>
          </DialogHeader>
          
          <ScrollArea className="h-[60vh] pr-4">
            {isLoadingSchema && (
              <div className="flex items-center justify-center py-8">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
              </div>
            )}
            
            {!isLoadingSchema && schemaDetails && (
              <div className="space-y-4">
                {/* Version Selector */}
                {schemaDetails.allVersions && schemaDetails.allVersions.length > 1 && (
                  <div className="border border-border rounded-lg p-4">
                    <div className="text-sm font-semibold mb-3">Available Versions ({schemaDetails.allVersions.length})</div>
                    <div className="flex flex-wrap gap-2">
                      {schemaDetails.allVersions.map((version: number) => (
                        <button
                          key={version}
                          onClick={() => handleVersionChange(version)}
                          className={cn(
                            "px-3 py-1.5 rounded-md text-sm font-medium transition-all",
                            version === schemaDetails.version
                              ? "bg-primary text-primary-foreground shadow-md"
                              : "bg-muted hover:bg-muted/70 text-muted-foreground hover:text-foreground"
                          )}
                        >
                          v{version}
                          {version === Math.max(...schemaDetails.allVersions) && (
                            <span className="ml-1 text-xs opacity-75">(latest)</span>
                          )}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
                
                <div className="grid grid-cols-3 gap-4 text-sm">
                  <div>
                    <div className="text-muted-foreground">Subject</div>
                    <div className="font-mono text-primary">{schemaDetails.subject}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground">Current Version</div>
                    <div className="font-semibold text-lg">v{schemaDetails.version}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground">Type</div>
                    <div className="font-semibold">{schemaDetails.schemaType || 'AVRO'}</div>
                  </div>
                  {schemaDetails.id && (
                    <div>
                      <div className="text-muted-foreground">Schema ID</div>
                      <div className="font-mono">{schemaDetails.id}</div>
                    </div>
                  )}
                </div>
                
                <div>
                  <div className="text-sm font-semibold mb-2">Schema Definition</div>
                  <pre className="bg-muted p-4 rounded-lg text-xs font-mono overflow-auto whitespace-pre-wrap">
                    {(() => {
                      const schema = schemaDetails.schema;
                      const schemaType = schemaDetails.schemaType || 'AVRO';
                      
                      // PROTOBUF schemas are plain text, don't parse as JSON
                      if (schemaType === 'PROTOBUF') {
                        return schema;
                      }
                      
                      // AVRO and JSON schemas are JSON strings
                      try {
                        if (typeof schema === 'string') {
                          return JSON.stringify(JSON.parse(schema), null, 2);
                        }
                        return JSON.stringify(schema, null, 2);
                      } catch (e) {
                        // If parsing fails, return raw schema
                        return schema;
                      }
                    })()}
                  </pre>
                </div>
              </div>
            )}
        </ScrollArea>
      </DialogContent>
    </Dialog>

    {/* ACL Details Dialog */}
    <Dialog open={showAclDialog} onOpenChange={setShowAclDialog}>
      <DialogContent className="max-w-2xl max-h-[80vh]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Shield className="w-5 h-5 text-[hsl(var(--node-acl))]" />
            ACL: {data.topic || 'Topic'}
          </DialogTitle>
        </DialogHeader>
        <ScrollArea className="max-h-[60vh] pr-4">
          {Array.isArray(data.acls) && data.acls.length > 0 ? (
            <div className="space-y-3">
              <p className="text-sm text-muted-foreground">
                {data.acls.length} binding(s) for topic <span className="font-mono font-medium text-foreground">{data.topic}</span>
              </p>
              <div className="rounded-lg border border-border overflow-hidden">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border bg-muted/50">
                      <th className="text-left py-2 px-3 font-semibold">Principal</th>
                      <th className="text-left py-2 px-3 font-semibold">Host</th>
                      <th className="text-left py-2 px-3 font-semibold">Operation</th>
                      <th className="text-left py-2 px-3 font-semibold">Permission</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.acls.map((acl: { principal?: string; host?: string; operation?: string; permissionType?: string }, i: number) => (
                      <tr key={i} className="border-b border-border/50 last:border-0">
                        <td className="py-2 px-3 font-mono text-xs">{acl.principal ?? '—'}</td>
                        <td className="py-2 px-3 font-mono text-xs">{acl.host ?? '—'}</td>
                        <td className="py-2 px-3">{acl.operation ?? '—'}</td>
                        <td className="py-2 px-3">{acl.permissionType ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">No ACL bindings.</p>
          )}
        </ScrollArea>
      </DialogContent>
    </Dialog>

    {/* Consumer Lag Dialog */}
    <Dialog open={showConsumerLagDialog} onOpenChange={setShowConsumerLagDialog}>
      <DialogContent className="max-w-4xl max-h-[80vh]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Activity className="w-5 h-5 text-green-500" />
            Consumer Lag: {data.label}
          </DialogTitle>
        </DialogHeader>
        
        <ScrollArea className="h-[60vh] pr-4">
          {isLoadingLag && (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
            </div>
          )}
          
          {!isLoadingLag && consumerLag && (
            <div className="space-y-6">
              {Object.entries(consumerLag.topics || {}).map(([topicName, topicData]: [string, any]) => {
                const totalLag = topicData.partitions?.reduce((sum: number, p: any) => sum + (p.lag || 0), 0) || 0;
                
                return (
                  <div key={topicName} className="border border-border rounded-lg p-4">
                    <div className="flex items-center justify-between mb-3">
                      <h3 className="font-semibold text-lg flex items-center gap-2">
                        <span className="text-primary">{topicName}</span>
                      </h3>
                      <div className="text-sm">
                        <span className="text-muted-foreground">Total Lag: </span>
                        <span className={cn(
                          "font-bold",
                          totalLag === 0 ? "text-green-400" : totalLag < 1000 ? "text-yellow-400" : "text-red-400"
                        )}>
                          {totalLag.toLocaleString()}
                        </span>
                      </div>
                    </div>
                    
                    <div className="space-y-2">
                      {topicData.partitions?.sort((a: any, b: any) => a.partition - b.partition).map((partition: any) => (
                        <div key={partition.partition} className="bg-muted/30 rounded p-3 grid grid-cols-4 gap-4 text-sm">
                          <div>
                            <div className="text-muted-foreground text-xs">Partition</div>
                            <div className="font-bold">{partition.partition}</div>
                          </div>
                          <div>
                            <div className="text-muted-foreground text-xs">Current Offset</div>
                            <div className="font-mono">{partition.currentOffset?.toLocaleString() || 'N/A'}</div>
                          </div>
                          <div>
                            <div className="text-muted-foreground text-xs">Log End Offset</div>
                            <div className="font-mono">{partition.logEndOffset?.toLocaleString() || 'N/A'}</div>
                          </div>
                          <div>
                            <div className="text-muted-foreground text-xs">Lag</div>
                            <div className={cn(
                              "font-bold",
                              partition.lag === 0 ? "text-green-400" : partition.lag < 100 ? "text-yellow-400" : "text-red-400"
                            )}>
                              {partition.lag?.toLocaleString() || '0'}
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })}
              
              {!consumerLag?.topics || Object.keys(consumerLag.topics).length === 0 && (
                <div className="text-center py-8 text-muted-foreground">
                  No lag information available for this consumer group
                </div>
              )}
            </div>
          )}
        </ScrollArea>
      </DialogContent>
    </Dialog>

    {/* Topic Details Dialog */}
    <Dialog open={showTopicDialog} onOpenChange={setShowTopicDialog}>
      <DialogContent className="max-w-4xl max-h-[80vh]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <CylinderIcon className="w-5 h-5 text-purple-500" />
            Topic: {data.label}
          </DialogTitle>
        </DialogHeader>
        
        <ScrollArea className="h-[60vh] pr-4">
          {isLoadingTopic && (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
            </div>
          )}
          
          {!isLoadingTopic && topicDetails && (
            <div className="space-y-6">
              {/* Topic Configuration */}
              <div className="border border-border rounded-lg p-4">
                <h3 className="font-semibold text-lg mb-4">Configuration</h3>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <div className="text-muted-foreground text-xs mb-1">Partitions</div>
                    <div className="font-bold text-lg">{topicDetails.partitions}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground text-xs mb-1">Replication Factor</div>
                    <div className="font-bold text-lg">{topicDetails.replicationFactor}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground text-xs mb-1">Cleanup Policy</div>
                    <div className="font-mono text-sm">
                      <span className={cn(
                        "px-2 py-1 rounded",
                        topicDetails.config?.cleanupPolicy === 'compact' ? "bg-blue-950/30 text-blue-400" : "bg-red-950/30 text-red-400"
                      )}>
                        {topicDetails.config?.cleanupPolicy || 'delete'}
                      </span>
                    </div>
                  </div>
                  <div>
                    <div className="text-muted-foreground text-xs mb-1">Retention Time</div>
                    <div className="font-mono text-sm">{topicDetails.config?.retentionMsDisplay || 'N/A'}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground text-xs mb-1">Retention Bytes</div>
                    <div className="font-mono text-sm">{topicDetails.config?.retentionBytes === '-1' ? 'Unlimited' : topicDetails.config?.retentionBytes || 'N/A'}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground text-xs mb-1">Max Message Size</div>
                    <div className="font-mono text-sm">
                      {topicDetails.config?.maxMessageBytes ? `${(parseInt(topicDetails.config.maxMessageBytes) / 1024 / 1024).toFixed(2)} MB` : 'N/A'}
                    </div>
                  </div>
                </div>
              </div>

              {/* Generate client code */}
              <div className="border border-border rounded-lg p-4">
                <h3 className="font-semibold text-lg mb-4 flex items-center gap-2">
                  <Code className="w-5 h-5 text-primary" />
                  Generate client code
                </h3>
                <div className="flex flex-wrap items-end gap-3 mb-4">
                  <div className="space-y-1.5">
                    <Label className="text-xs text-muted-foreground">Client</Label>
                    <Select value={codeGenClient} onValueChange={(v: 'producer' | 'consumer' | 'streams') => setCodeGenClient(v)}>
                      <SelectTrigger className="w-[130px]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="producer">Producer</SelectItem>
                        <SelectItem value="consumer">Consumer</SelectItem>
                        <SelectItem value="streams">Kafka Streams</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  {codeGenClient !== 'streams' && (
                    <div className="space-y-1.5">
                      <Label className="text-xs text-muted-foreground">Language</Label>
                      <Select value={codeGenLanguage} onValueChange={(v: 'java' | 'python') => setCodeGenLanguage(v)}>
                        <SelectTrigger className="w-[120px]">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="java">Java</SelectItem>
                          <SelectItem value="python">Python</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  )}
                  {codeGenClient === 'streams' && (
                    <div className="space-y-1.5">
                      <Label className="text-xs text-muted-foreground">Output topic</Label>
                      <Input
                        placeholder={(topicDetails?.name || data.label || '') + '-processed'}
                        value={codeGenOutputTopic}
                        onChange={(e) => setCodeGenOutputTopic(e.target.value)}
                        className="w-[180px] font-mono text-sm"
                      />
                    </div>
                  )}
                  {codeGenClient !== 'streams' && (
                    <div className="flex items-center gap-2">
                      <input
                        type="checkbox"
                        id="code-schema-registry"
                        checked={codeGenSchemaRegistry}
                        onChange={(e) => setCodeGenSchemaRegistry(e.target.checked)}
                        className="rounded border-border"
                      />
                      <Label htmlFor="code-schema-registry" className="text-sm cursor-pointer">With Schema Registry</Label>
                    </div>
                  )}
                  <Button
                    onClick={handleGenerateCode}
                    disabled={isLoadingCode}
                    size="sm"
                    className="gap-2"
                  >
                    {isLoadingCode ? (
                      <div className="animate-spin rounded-full h-3 w-3 border-2 border-primary border-t-transparent" />
                    ) : (
                      <Code className="w-4 h-4" />
                    )}
                    Generate
                  </Button>
                </div>
                {generatedCode && (
                  <div className="relative">
                    <pre className="bg-muted p-4 rounded-lg text-xs font-mono overflow-auto max-h-80 whitespace-pre-wrap border border-border">
                      {generatedCode}
                    </pre>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      className="absolute top-2 right-2 gap-1.5"
                      onClick={handleCopyCode}
                    >
                      <Copy className="w-3.5 h-3.5" />
                      Copy
                    </Button>
                  </div>
                )}
              </div>

              {/* Produce message - only when enabled by config, for user topics, and no connector attached */}
              {!isInternalTopic && data.enableProduceFromUi && !data.hasConnector && (
                <div className="border border-border rounded-lg p-4">
                  <h3 className="font-semibold text-lg mb-4 flex items-center gap-2">
                    <Send className="w-5 h-5 text-primary" />
                    Produce message
                  </h3>
                  <div className="space-y-4">
                    <div>
                      <Label htmlFor="produce-value" className="text-sm">Message (required)</Label>
                      <Textarea
                        id="produce-value"
                        placeholder="Enter message text..."
                        value={produceValue}
                        onChange={(e) => setProduceValue(e.target.value)}
                        className="mt-1.5 min-h-[80px] font-mono text-sm resize-y"
                        disabled={isProducing}
                      />
                    </div>
                    <div>
                      <Label htmlFor="produce-key" className="text-sm text-muted-foreground">Key (optional)</Label>
                      <Input
                        id="produce-key"
                        placeholder="Optional message key"
                        value={produceKey}
                        onChange={(e) => setProduceKey(e.target.value)}
                        className="mt-1.5 font-mono"
                        disabled={isProducing}
                      />
                    </div>
                    <Button
                      onClick={handleProduce}
                      disabled={isProducing || !produceValue.trim()}
                      size="sm"
                      className="gap-2"
                    >
                      {isProducing ? (
                        <>
                          <div className="animate-spin rounded-full h-3 w-3 border-2 border-primary border-t-transparent" />
                          Producing...
                        </>
                      ) : (
                        <>
                          <Send className="w-4 h-4" />
                          Produce
                        </>
                      )}
                    </Button>
                  </div>
                </div>
              )}

              {/* Recent Messages - Load on Demand */}
              <div className="border border-border rounded-lg p-4">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="font-semibold text-lg">Recent Messages (Last 5)</h3>
                  {!messagesLoaded && (
                    <Button
                      onClick={handleLoadMessages}
                      disabled={isLoadingMessages}
                      size="sm"
                      variant="outline"
                      className="gap-2"
                    >
                      {isLoadingMessages ? (
                        <>
                          <div className="animate-spin rounded-full h-3 w-3 border-b-2 border-primary"></div>
                          Loading...
                        </>
                      ) : (
                        'View Messages'
                      )}
                    </Button>
                  )}
                </div>
                
                {!messagesLoaded && !isLoadingMessages && (
                  <div className="text-center py-8 text-muted-foreground">
                    Click "View Messages" to load the last 5 messages from this topic
                  </div>
                )}
                
                {messagesLoaded && topicDetails.recentMessages && topicDetails.recentMessages.length > 0 && (
                  <div className="space-y-3">
                    {topicDetails.recentMessages.map((msg: any, idx: number) => (
                      <div key={idx} className="bg-muted/30 rounded p-3 space-y-2">
                        <div className="flex items-center justify-between text-xs text-muted-foreground">
                          <div className="flex items-center gap-4">
                            <span>Partition: <span className="font-bold text-foreground">{msg.partition}</span></span>
                            <span>Offset: <span className="font-bold text-foreground">{msg.offset}</span></span>
                          </div>
                          {msg.timestamp && (
                            <span>{new Date(msg.timestamp).toLocaleString()}</span>
                          )}
                        </div>
                        {msg.key && (
                          <div>
                            <div className="text-xs text-muted-foreground mb-1">Key:</div>
                            <div className="bg-background p-2 rounded font-mono text-xs break-all">{msg.key}</div>
                          </div>
                        )}
                        <div>
                          <div className="text-xs text-muted-foreground mb-1">Value:</div>
                          <div className="bg-background p-2 rounded font-mono text-xs break-all max-h-32 overflow-y-auto">
                            {msg.value || '<null>'}
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
                
                {messagesLoaded && (!topicDetails.recentMessages || topicDetails.recentMessages.length === 0) && (
                  <div className="text-center py-8 text-muted-foreground">
                    No recent messages found. This topic may be empty or no producers are currently active.
                  </div>
                )}
              </div>
            </div>
          )}
        </ScrollArea>
      </DialogContent>
    </Dialog>

    {/* Connector Details Dialog */}
    <Dialog open={showConnectorDialog} onOpenChange={setShowConnectorDialog}>
      <DialogContent className="max-w-3xl max-h-[80vh]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <ArrowRightLeft className="w-5 h-5 text-orange-500" />
            Connector: {data.label}
          </DialogTitle>
        </DialogHeader>
        
        <ScrollArea className="h-[60vh] pr-4">
          {isLoadingConnector && (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
            </div>
          )}
          
          {!isLoadingConnector && connectorDetails && (
            <div className="space-y-6">
              {/* Connector Info */}
              <div className="border border-border rounded-lg p-4">
                <h3 className="font-semibold text-lg mb-4">Connector Information</h3>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <div className="text-muted-foreground text-xs mb-1">Name</div>
                    <div className="font-mono text-sm">{connectorDetails.name}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground text-xs mb-1">Type</div>
                    <div className="font-mono text-sm">
                      <span className={cn(
                        "px-2 py-1 rounded",
                        connectorDetails.type === 'source' ? "bg-green-950/30 text-green-400" : "bg-blue-950/30 text-blue-400"
                      )}>
                        {connectorDetails.type}
                      </span>
                    </div>
                  </div>
                  <div className="col-span-2">
                    <div className="text-muted-foreground text-xs mb-1">Connector Class</div>
                    <div className="font-mono text-xs break-all">{connectorDetails.connectorClass}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground text-xs mb-1">Tasks</div>
                    <div className="font-bold text-lg">{connectorDetails.tasks?.length || 0}</div>
                  </div>
                </div>
              </div>

              {/* Connector Configuration */}
              <div className="border border-border rounded-lg p-4">
                <h3 className="font-semibold text-lg mb-4">Configuration</h3>
                <div className="space-y-2 max-h-96 overflow-y-auto">
                  {Object.entries(connectorDetails.config || {}).map(([key, value]: [string, any]) => (
                    <div key={key} className="bg-muted/30 rounded p-3">
                      <div className="text-xs text-muted-foreground mb-1 font-semibold">{key}</div>
                      <div className={cn(
                        "font-mono text-xs break-all",
                        value === '********' ? "text-yellow-400" : "text-foreground"
                      )}>
                        {String(value)}
                        {value === '********' && (
                          <span className="ml-2 text-[10px] text-yellow-400/70">(masked for security)</span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </ScrollArea>
      </DialogContent>
    </Dialog>
  </>
  );
});
