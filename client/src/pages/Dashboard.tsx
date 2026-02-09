import { Link } from "wouter";
import { useClusters, useClusterHealth } from "@/hooks/use-kafka";
import { ThemeToggle } from "@/components/ThemeToggle";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Server, ArrowRight, Database, XCircle, ArrowRightLeft, Shield, Sparkles } from "lucide-react";
import { motion } from "framer-motion";
import { formatDistanceToNow } from "date-fns";

function ClusterCard({ cluster }: { cluster: any }) {
  const { data: health } = useClusterHealth(cluster.id);

  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35, ease: [0.25, 0.46, 0.45, 0.94] }}
    >
      <Card className="group h-full overflow-hidden border border-border/60 bg-card/40 backdrop-blur-sm transition-all duration-300 hover:border-primary/30 hover:shadow-2xl hover:shadow-primary/5 hover:bg-card/60">
        {/* Status accent bar */}
        <div
          className={`h-1 w-full ${health?.online ? "bg-gradient-to-r from-emerald-500 to-emerald-400" : "bg-gradient-to-r from-rose-500/80 to-rose-400/80"}`}
        />
        <CardHeader className="pb-3 pt-5">
          <div className="flex items-start justify-between gap-2">
            <div className="flex items-center gap-3 min-w-0">
              <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-primary/20 to-primary/5 ring-1 ring-primary/20">
                <img src="/streamlens-logo.svg" alt="" className="h-6 w-6 object-contain" />
              </div>
              <div className="min-w-0">
                <CardTitle className="text-xl font-semibold tracking-tight truncate">{cluster.name}</CardTitle>
                <div className="flex items-center gap-2 mt-1">
                  {health?.online ? (
                    <span className="inline-flex items-center gap-1 rounded-full bg-emerald-500/15 px-2 py-0.5 text-xs font-medium text-emerald-400 ring-1 ring-emerald-500/20">
                      <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse" />
                      Online
                    </span>
                  ) : (
                    <span className="inline-flex items-center gap-1 rounded-full bg-rose-500/15 px-2 py-0.5 text-xs font-medium text-rose-400 ring-1 ring-rose-500/20">
                      <XCircle className="h-3 w-3" />
                      Offline
                    </span>
                  )}
                  {health?.clusterMode && (
                    <span className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ring-1 ${health.clusterMode === "kraft" ? "bg-emerald-500/10 text-emerald-400 ring-emerald-500/20" : "bg-amber-500/10 text-amber-400 ring-amber-500/20"}`}>
                      <Shield className="h-3 w-3" />
                      {health.clusterMode === "kraft" ? "KRaft" : "Zookeeper"}
                    </span>
                  )}
                </div>
              </div>
            </div>
          </div>
          <CardDescription className="mt-3 space-y-2">
            <div className="flex items-center gap-2 rounded-lg bg-muted/40 px-2.5 py-1.5 font-mono text-xs text-muted-foreground" title={cluster.bootstrapServers}>
              <Server className="h-3.5 w-3.5 shrink-0 text-muted-foreground/80" />
              <span className="truncate">{cluster.bootstrapServers}</span>
            </div>
            {cluster.schemaRegistryUrl && (
              <div className="flex items-center gap-2 font-mono text-xs text-blue-400/90 truncate" title={cluster.schemaRegistryUrl}>
                <Database className="h-3.5 w-3.5 shrink-0" />
                <span className="truncate">{cluster.schemaRegistryUrl}</span>
              </div>
            )}
            {cluster.connectUrl && (
              <div className="flex items-center gap-2 font-mono text-xs text-violet-400/90 truncate" title={cluster.connectUrl}>
                <ArrowRightLeft className="h-3.5 w-3.5 shrink-0" />
                <span className="truncate">{cluster.connectUrl}</span>
              </div>
            )}
          </CardDescription>
        </CardHeader>
        <CardContent className="pt-0">
          <div className="flex items-center justify-between border-t border-border/60 pt-4">
            <span className="text-xs text-muted-foreground">
              Added {formatDistanceToNow(new Date(cluster.createdAt || new Date()), { addSuffix: true })}
            </span>
            {health?.online ? (
              <Link href={`/topology/${cluster.id}`} className="block">
                <Button className="rounded-lg bg-primary px-4 font-medium text-primary-foreground shadow-lg shadow-primary/25 transition-all hover:bg-primary/90 hover:shadow-primary/30 group-hover:scale-[1.02]">
                  View Topology
                  <ArrowRight className="ml-2 h-4 w-4 transition-transform group-hover:translate-x-0.5" />
                </Button>
              </Link>
            ) : (
              <Button disabled className="rounded-lg opacity-60 cursor-not-allowed" title="Cluster is offline">
                View Topology
                <ArrowRight className="ml-2 h-4 w-4" />
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

  if (isLoading) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="flex flex-col items-center gap-6">
          <div className="relative">
            <div className="h-14 w-14 rounded-2xl border-2 border-primary/30 border-t-primary animate-spin" />
            <div className="absolute inset-0 h-14 w-14 rounded-2xl bg-primary/5 blur-xl" />
          </div>
          <p className="text-muted-foreground text-sm font-medium">Loading clusters...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background text-foreground">
      {/* Subtle background gradient */}
      <div className="fixed inset-0 -z-10 bg-[radial-gradient(ellipse_80%_50%_at_50%_-20%,hsl(var(--primary)/0.08),transparent)]" />
      <div className="fixed inset-0 -z-10 bg-[radial-gradient(ellipse_60%_40%_at_80%_100%,hsl(var(--primary)/0.04),transparent)]" />

      <div className="relative mx-auto max-w-7xl px-6 py-10 sm:px-8 sm:py-12 lg:px-10">
        {/* Hero */}
        <motion.header
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4 }}
          className="flex flex-col gap-8 sm:flex-row sm:items-end sm:justify-between"
        >
          <div className="flex items-start gap-5">
            <div className="relative flex h-16 w-16 shrink-0 items-center justify-center rounded-2xl bg-gradient-to-br from-primary/25 to-primary/5 ring-1 ring-primary/20 shadow-lg shadow-primary/10">
              <img src="/streamlens-logo.svg" alt="" className="h-9 w-9 object-contain" />
              <div className="absolute -inset-1 rounded-2xl bg-gradient-to-br from-primary/10 to-transparent blur-xl -z-10" />
            </div>
            <div className="space-y-1 pt-0.5">
              <h2 className="text-4xl font-extrabold tracking-tight sm:text-3xl">
                <span className="bg-gradient-to-r from-primary to-primary/60 bg-clip-text text-transparent">
                  streamLens
                </span>
              </h2>
              <p className="max-w-sm text-sm leading-snug text-muted-foreground">
                Visualize, analyze your Apache Kafka streaming platform.
              </p>
            </div>
          </div>
          <div className="flex shrink-0 items-center gap-2">
            <ThemeToggle />
          </div>
        </motion.header>

        {/* Content */}
        {showEmptyState ? (
          <motion.section
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.4, delay: 0.1 }}
            className="mt-14 flex flex-col items-center justify-center rounded-3xl border-2 border-dashed border-border/80 bg-card/20 px-8 py-16 text-center backdrop-blur-sm sm:py-20"
          >
            <div className="flex h-20 w-20 items-center justify-center rounded-2xl bg-muted/50 ring-1 ring-border/60">
              <Server className="h-10 w-10 text-muted-foreground/70" />
            </div>
            <h2 className="mt-6 text-xl font-semibold sm:text-2xl">No clusters configured</h2>
            <p className="mt-2 max-w-sm text-muted-foreground">
              Add clusters to <code className="rounded bg-muted px-1.5 py-0.5 text-xs">server/data/clusters.json</code> and restart the server.
            </p>
          </motion.section>
        ) : (
          <motion.section
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.4, delay: 0.1 }}
            className="mt-14 space-y-8"
          >
            <div className="flex flex-wrap items-center gap-3">
              <h2 className="text-2xl font-bold tracking-tight sm:text-3xl">Your clusters</h2>
              <span className="inline-flex items-center gap-1.5 rounded-full bg-primary/10 px-3 py-1 text-sm font-medium text-primary">
                <Sparkles className="h-4 w-4" />
                {clusters?.length} {clusters?.length === 1 ? "cluster" : "clusters"}
              </span>
              <img
                src="https://kafka.apache.org/logos/kafka_logo--simple.png"
                alt=""
                className="h-5 opacity-50 brightness-0 dark:brightness-100 dark:invert"
              />
            </div>
            <div className="grid gap-6 sm:grid-cols-2 xl:grid-cols-3">
              {clusters?.map((cluster) => (
                <ClusterCard key={cluster.id} cluster={cluster} />
              ))}
            </div>
          </motion.section>
        )}
      </div>
    </div>
  );
}
