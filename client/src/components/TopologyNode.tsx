import { memo } from 'react';
import { Handle, Position } from 'reactflow';
import { Activity, Box, ArrowRightLeft, FileJson, GitBranch, Sparkles, Shield, User, Zap } from 'lucide-react';
import { cn } from '@/lib/utils';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';

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
    default: return null;
  }
};

const SourceBadge = ({ source }: { source?: string }) => {
  if (!source) return null;
  
  switch (source) {
    case 'auto-discovered':
      return (
        <div className="flex items-center gap-1 text-[10px] text-green-400 bg-green-950/30 px-1.5 py-0.5 rounded border border-green-900/50">
          <Sparkles className="w-3 h-3" />
          <span>Live</span>
        </div>
      );
    case 'config':
      return (
        <div className="flex items-center gap-1 text-[10px] text-purple-400 bg-purple-950/30 px-1.5 py-0.5 rounded border border-purple-900/50">
          <User className="w-3 h-3" />
          <span>Config</span>
        </div>
      );
    case 'jmx':
      return (
        <div className="flex items-center gap-1 text-[10px] text-yellow-400 bg-yellow-950/30 px-1.5 py-0.5 rounded border border-yellow-900/50">
          <Zap className="w-3 h-3" />
          <span>JMX</span>
        </div>
      );
    case 'acl':
      return (
        <div className="flex items-center gap-1 text-[10px] text-amber-400 bg-amber-950/30 px-1.5 py-0.5 rounded border border-amber-900/50">
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

  return (
    <div className={cn(
      "min-w-[180px] px-4 py-3 rounded-xl bg-card border-2 transition-all duration-300 shadow-xl",
      selected ? "border-primary ring-4 ring-primary/10" : "border-border",
      isHighlighted ? "border-primary shadow-[0_0_20px_hsl(var(--primary)/0.3)] scale-105" : "",
      isSearchMatch ? "ring-2 ring-yellow-500/50" : "",
      "hover:border-primary/50"
    )}>
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
  );
});
