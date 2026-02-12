import { useState, useEffect, useRef } from "react";
import { useAiQuery, useAiStatus } from "@/hooks/use-kafka";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Loader2, Send, Sparkles, Bot, AlertCircle } from "lucide-react";
import { cn } from "@/lib/utils";
import { motion, AnimatePresence } from "framer-motion";

interface AiChatPanelProps {
  topology: any;
  onHighlightNodes: (nodeIds: string[]) => void;
  /** Fallback: search the full cluster for a topic when AI can't find it in loaded nodes */
  onSearchAndNavigate?: (query: string) => Promise<void>;
}

interface Message {
  role: "user" | "assistant";
  content: string;
  timestamp: number;
}

const PROVIDER_LABELS: Record<string, string> = {
  openai: "OpenAI",
  gemini: "Gemini",
  anthropic: "Claude",
  ollama: "Ollama",
};

export function AiChatPanel({ topology, onHighlightNodes, onSearchAndNavigate }: AiChatPanelProps) {
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState<Message[]>([
    { 
      role: "assistant", 
      content: "👋 Hi! I'm StreamPilot, your AI assistant for navigating Kafka topologies. I can answer questions and automatically highlight & zoom to relevant nodes.\n\nTry asking:\n• 'Which producers write to testtopic?'\n• 'Show me all consumers of orders topic'\n• 'What topics does my-app produce to?'\n• 'Which topics have registered schemas?'\n• 'Do we have any source or sink connectors?'",
      timestamp: Date.now() 
    }
  ]);
  const scrollRef = useRef<HTMLDivElement>(null);
  
  const aiQuery = useAiQuery();
  const { data: aiStatus } = useAiStatus();

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [messages]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || !topology) return;

    const userMessage: Message = { role: "user", content: input, timestamp: Date.now() };
    setMessages(prev => [...prev, userMessage]);
    setInput("");

    try {
      const result = await aiQuery.mutateAsync({
        question: userMessage.content,
        topology: topology.data || topology, // Handle structure
      });

      const aiMessage: Message = { role: "assistant", content: result.answer, timestamp: Date.now() };
      setMessages(prev => [...prev, aiMessage]);
      
      // Highlight nodes referenced in the answer
      if (result.highlightNodes && result.highlightNodes.length > 0) {
        onHighlightNodes(result.highlightNodes);
      } else if (onSearchAndNavigate) {
        // AI returned no highlights — try a full-cluster search as fallback
        // so topics not in the loaded subset can still be found
        await onSearchAndNavigate(userMessage.content);
      }
    } catch (error) {
      setMessages(prev => [...prev, { 
        role: "assistant", 
        content: "Sorry, I encountered an error analyzing the topology.", 
        timestamp: Date.now() 
      }]);
    }
  };

  return (
    <Card className="flex flex-col h-full border-l border-border rounded-none bg-card/50 backdrop-blur-sm">
      <CardHeader className="border-b border-border py-3 px-4 bg-muted/20">
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2 text-sm font-bold uppercase tracking-widest text-primary">
            <Sparkles className="w-4 h-4" />
            StreamPilot
          </CardTitle>
          {aiStatus?.configured ? (
            <span className="inline-flex items-center gap-1.5 rounded-full bg-primary/10 px-2.5 py-0.5 text-[10px] font-medium text-primary ring-1 ring-primary/20">
              <span className="h-1.5 w-1.5 rounded-full bg-primary animate-pulse" />
              {PROVIDER_LABELS[aiStatus.provider || ""] || aiStatus.provider}
              {aiStatus.model && (
                <span className="text-muted-foreground font-normal">· {aiStatus.model}</span>
              )}
            </span>
          ) : (
            <span className="inline-flex items-center gap-1.5 rounded-full bg-yellow-100 dark:bg-yellow-950/30 px-2.5 py-0.5 text-[10px] font-medium text-yellow-700 dark:text-yellow-400 ring-1 ring-yellow-300 dark:ring-yellow-900/50">
              <AlertCircle className="w-3 h-3" />
              Not configured
            </span>
          )}
        </div>
      </CardHeader>
      
      <ScrollArea className="flex-1 p-4 overflow-y-auto">
        <div className="flex flex-col gap-4 w-full">
          <AnimatePresence initial={false}>
            {messages.map((msg, idx) => (
              <motion.div
                key={msg.timestamp}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                className={cn(
                  "flex gap-3 max-w-[90%] min-w-0",
                  msg.role === "user" ? "ml-auto flex-row-reverse" : ""
                )}
              >
                <div className={cn(
                  "w-8 h-8 rounded-full flex items-center justify-center shrink-0 border",
                  msg.role === "assistant" 
                    ? "bg-primary/10 border-primary/20 text-primary" 
                    : "bg-muted border-border text-muted-foreground"
                )}>
                  {msg.role === "assistant" ? <Bot className="w-4 h-4" /> : <div className="w-2 h-2 rounded-full bg-current" />}
                </div>
                <div className={cn(
                  "p-3 rounded-2xl text-sm leading-relaxed shadow-sm whitespace-pre-wrap break-words min-w-0 flex-1",
                  msg.role === "assistant" 
                    ? "bg-card border border-border/50 text-foreground rounded-tl-none" 
                    : "bg-primary text-primary-foreground rounded-tr-none"
                )}>
                  {msg.content}
                </div>
              </motion.div>
            ))}
            {aiQuery.isPending && (
              <motion.div 
                initial={{ opacity: 0 }} 
                animate={{ opacity: 1 }}
                className="flex gap-3"
              >
                <div className="w-8 h-8 rounded-full bg-primary/10 border border-primary/20 flex items-center justify-center shrink-0">
                  <Bot className="w-4 h-4 text-primary" />
                </div>
                <div className="bg-card border border-border/50 p-4 rounded-2xl rounded-tl-none flex items-center gap-2">
                  <Loader2 className="w-4 h-4 animate-spin text-primary" />
                  <span className="text-xs text-muted-foreground">Analyzing topology...</span>
                </div>
              </motion.div>
            )}
            <div ref={scrollRef} />
          </AnimatePresence>
        </div>
      </ScrollArea>

      <div className="p-4 border-t border-border bg-background">
        <form onSubmit={handleSubmit} className="flex gap-2 relative">
          <Input 
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about your Kafka topology..."
            className="pr-12 bg-muted/50 border-border focus-visible:ring-primary"
            disabled={aiQuery.isPending}
          />
          <Button 
            type="submit" 
            size="icon"
            disabled={!input.trim() || aiQuery.isPending}
            className="absolute right-1 top-1 h-8 w-8 bg-primary hover:bg-primary/90"
          >
            <Send className="w-4 h-4" />
          </Button>
        </form>
      </div>
    </Card>
  );
}
