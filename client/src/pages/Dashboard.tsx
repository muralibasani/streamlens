import { useState } from "react";
import { Link } from "wouter";
import { useClusters, useDeleteCluster, useClusterHealth } from "@/hooks/use-kafka";
import { CreateClusterDialog } from "@/components/CreateClusterDialog";
import { EditClusterDialog } from "@/components/EditClusterDialog";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { MoreHorizontal, Server, Activity, Trash2, ArrowRight, Database, Edit, XCircle, ArrowRightLeft } from "lucide-react";
import { motion } from "framer-motion";
import { formatDistanceToNow } from "date-fns";

function ClusterCard({ cluster, onEdit, onDelete }: { cluster: any; onEdit: (cluster: any) => void; onDelete: (id: number) => void }) {
  const { data: health } = useClusterHealth(cluster.id);
  
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
    >
      <Card className="group h-full hover:border-primary/50 transition-all duration-300 hover:shadow-xl hover:shadow-primary/5 bg-card/50 backdrop-blur-sm">
        <CardHeader className="pb-4">
          <div className="flex items-start justify-between">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-gradient-to-br from-orange-500/10 to-blue-500/10">
                <img 
                  src="/streamlens-logo.svg" 
                  alt="StreamLens" 
                  className="w-6 h-6 object-contain"
                />
              </div>
              <CardTitle className="text-2xl font-bold tracking-tight">{cluster.name}</CardTitle>
            </div>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="ghost" className="h-8 w-8 p-0 text-muted-foreground hover:text-foreground">
                  <span className="sr-only">Open menu</span>
                  <MoreHorizontal className="h-4 w-4" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="bg-card border-border">
                <DropdownMenuItem 
                  className="cursor-pointer"
                  onClick={() => onEdit(cluster)}
                >
                  <Edit className="mr-2 h-4 w-4" />
                  Edit Cluster
                </DropdownMenuItem>
                <DropdownMenuItem 
                  className="text-destructive focus:text-destructive focus:bg-destructive/10 cursor-pointer"
                  onClick={() => onDelete(cluster.id)}
                >
                  <Trash2 className="mr-2 h-4 w-4" />
                  Delete Cluster
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
          <CardDescription className="space-y-1">
            <div className="font-mono text-xs truncate flex items-center gap-2" title={cluster.bootstrapServers}>
              <Server className="w-3 h-3 flex-shrink-0" />
              <span className="truncate">{cluster.bootstrapServers}</span>
            </div>
            {cluster.schemaRegistryUrl && (
              <div className="font-mono text-xs truncate flex items-center gap-2 text-blue-400" title={cluster.schemaRegistryUrl}>
                <Database className="w-3 h-3 flex-shrink-0" />
                <span className="truncate">{cluster.schemaRegistryUrl}</span>
              </div>
            )}
            {cluster.connectUrl && (
              <div className="font-mono text-xs truncate flex items-center gap-2 text-purple-400" title={cluster.connectUrl}>
                <ArrowRightLeft className="w-3 h-3 flex-shrink-0" />
                <span className="truncate">{cluster.connectUrl}</span>
              </div>
            )}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {health?.online ? (
              <div className="flex items-center gap-2 text-sm text-green-400">
                <Activity className="w-4 h-4" />
                <span>Status: Online</span>
              </div>
            ) : (
              <div className="flex items-center gap-2 text-sm text-red-400">
                <XCircle className="w-4 h-4" />
                <span>Status: Offline</span>
              </div>
            )}
            
            <div className="pt-2 flex items-center justify-between text-xs text-muted-foreground border-t border-border">
              <span>Added {formatDistanceToNow(new Date(cluster.createdAt || new Date()), { addSuffix: true })}</span>
            </div>

            {health?.online ? (
              <Link href={`/topology/${cluster.id}`} className="block w-full">
                <Button className="w-full group-hover:bg-primary group-hover:text-primary-foreground transition-colors">
                  View Topology
                  <ArrowRight className="ml-2 w-4 h-4 group-hover:translate-x-1 transition-transform" />
                </Button>
              </Link>
            ) : (
              <Button 
                disabled 
                className="w-full opacity-50 cursor-not-allowed"
                title="Cluster is offline"
              >
                View Topology
                <ArrowRight className="ml-2 w-4 h-4" />
              </Button>
            )}
          </div>
        </CardContent>
      </Card>
    </motion.div>
  );
}

export default function Dashboard() {
  const { data: clusters, isLoading, error } = useClusters();
  const showEmptyState = error || !clusters?.length;
  const deleteCluster = useDeleteCluster();
  const [editingCluster, setEditingCluster] = useState<any>(null);

  if (isLoading) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="flex flex-col items-center gap-4">
          <div className="w-12 h-12 rounded-full border-4 border-primary border-t-transparent animate-spin" />
          <p className="text-muted-foreground animate-pulse">Loading Kafka Clusters...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background text-foreground p-8">
      <div className="max-w-7xl mx-auto space-y-12">
        {/* Header */}
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-6">
          <div className="flex items-center gap-6">
            <div className="relative">
              <img 
                src="/streamlens-logo.svg" 
                alt="StreamLens Logo" 
                className="w-16 h-16 object-contain"
              />
              <div className="absolute inset-0 bg-gradient-to-br from-orange-500/20 to-blue-500/20 rounded-2xl blur-xl -z-10"></div>
            </div>
            <div className="space-y-2">
              <h1 className="text-5xl font-black tracking-tighter bg-gradient-to-br from-foreground to-foreground/40 bg-clip-text text-transparent">
                StreamLens
              </h1>
              <p className="text-muted-foreground text-lg font-medium">
                Visualize, analyze, and optimize your Apache Kafka event streaming platforms.
              </p>
              
            </div>
          </div>
          <CreateClusterDialog />
        </div>

        {/* Empty state: no clusters (or backend not reachable) — just show Add cluster */}
        {showEmptyState ? (
          <div className="py-20 text-center border-2 border-dashed border-border rounded-3xl bg-card/30">
            <Server className="w-12 h-12 mx-auto text-muted-foreground mb-4 opacity-50" />
            <h3 className="text-xl font-bold mb-2">Add cluster</h3>
            <p className="text-muted-foreground mb-6 max-w-sm mx-auto">
              Add a Kafka cluster to start visualizing your topic topology and data flow.
            </p>
            <CreateClusterDialog />
          </div>
        ) : (
          <div className="space-y-6">
            <div className="flex items-center gap-3">
              <h2 className="text-3xl font-bold tracking-tight">Your Kafka Clusters</h2>
              <img 
                src="https://kafka.apache.org/logos/kafka_logo--simple.png" 
                alt="Kafka" 
                className="h-6 object-contain opacity-40 brightness-0 dark:brightness-100 dark:invert"
              />
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {clusters?.map((cluster) => (
                <ClusterCard 
                key={cluster.id} 
                cluster={cluster} 
                onEdit={setEditingCluster} 
                onDelete={(id) => {
                  if (confirm("Are you sure? This cannot be undone.")) {
                    deleteCluster.mutate(id);
                  }
                }} 
              />
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Edit Cluster Dialog */}
      {editingCluster && (
        <EditClusterDialog
          open={!!editingCluster}
          onOpenChange={(open) => !open && setEditingCluster(null)}
          cluster={editingCluster}
        />
      )}
    </div>
  );
}
