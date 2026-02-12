import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, API_BASE, buildUrl, type InsertCluster } from "@/lib/api";

// ============================================
// CLUSTERS
// ============================================

export function useClusters() {
  return useQuery({
    queryKey: [api.clusters.list.path],
    queryFn: async () => {
      try {
        const res = await fetch(`${API_BASE}${api.clusters.list.path}`);
        if (!res.ok) throw new Error("Server error. Please try again later.");
        return api.clusters.list.responses[200].parse(await res.json());
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        if (msg === "Failed to fetch" || msg.includes("fetch")) {
          throw new Error("Couldn't connect to the server. Is the backend running?");
        }
        throw e;
      }
    },
    retry: 1,
  });
}

export function useCluster(id: number) {
  return useQuery({
    queryKey: [api.clusters.get.path, id],
    queryFn: async () => {
      const url = `${API_BASE}${buildUrl(api.clusters.get.path, { id })}`;
      const res = await fetch(url);
      if (res.status === 404) return null;
      if (!res.ok) throw new Error("Failed to fetch cluster");
      return api.clusters.get.responses[200].parse(await res.json());
    },
    retry: 1,
  });
}

export function useClusterHealth(id: number) {
  return useQuery({
    queryKey: [api.clusters.health.path, id],
    queryFn: async () => {
      const url = `${API_BASE}${buildUrl(api.clusters.health.path, { id })}`;
      const res = await fetch(url);
      if (res.status === 404) return null;
      if (!res.ok) throw new Error("Failed to check cluster health");
      return api.clusters.health.responses[200].parse(await res.json());
    },
    retry: 1,
    refetchInterval: 30000, // Check every 30 seconds
  });
}

export function useCreateCluster() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async (data: InsertCluster) => {
      const res = await fetch(`${API_BASE}${api.clusters.create.path}`, {
        method: api.clusters.create.method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) {
        if (res.status === 400) {
          const error = api.clusters.create.responses[400].parse(await res.json());
          throw new Error(error.message);
        }
        throw new Error("Failed to create cluster");
      }
      return api.clusters.create.responses[201].parse(await res.json());
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: [api.clusters.list.path] }),
  });
}

export function useUpdateCluster() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ id, data }: { id: number; data: InsertCluster }) => {
      const url = `${API_BASE}${buildUrl(api.clusters.update.path, { id })}`;
      const res = await fetch(url, {
        method: api.clusters.update.method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (res.status === 404) throw new Error("Cluster not found");
      if (!res.ok) {
        const error = await res.json().catch(() => ({ message: "Failed to update cluster" }));
        throw new Error(error.message || "Failed to update cluster");
      }
      return api.clusters.update.responses[200].parse(await res.json());
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.clusters.list.path] });
    },
  });
}

export function useDeleteCluster() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async (id: number) => {
      const url = `${API_BASE}${buildUrl(api.clusters.delete.path, { id })}`;
      const res = await fetch(url, { 
        method: api.clusters.delete.method,
      });
      if (res.status === 404) throw new Error("Cluster not found");
      if (!res.ok) throw new Error("Failed to delete cluster");
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: [api.clusters.list.path] }),
  });
}

// ============================================
// TOPOLOGY (paginated: first page via useQuery, subsequent pages via mutation)
// ============================================

/** Number of topics per page in the topology view. Override with VITE_TOPICS_PER_PAGE env var. */
export const TOPICS_PER_PAGE = Number(import.meta.env.VITE_TOPICS_PER_PAGE) || 50;

export function useTopology(clusterId: number, topicLimit: number = TOPICS_PER_PAGE) {
  return useQuery({
    queryKey: [api.topology.get.path, clusterId],
    queryFn: async () => {
      let url = `${API_BASE}${buildUrl(api.topology.get.path, { id: clusterId })}`;
      if (topicLimit > 0) {
        url += `?topic_offset=0&topic_limit=${topicLimit}`;
      }
      const res = await fetch(url);
      if (res.status === 404) return null; // No snapshot yet
      if (!res.ok) throw new Error("Failed to fetch topology");
      return api.topology.get.responses[200].parse(await res.json());
    },
  });
}

/** Fetch the next page of topics for incremental loading */
export function useLoadMoreTopics() {
  return useMutation({
    mutationFn: async ({
      clusterId,
      offset,
      limit,
    }: {
      clusterId: number;
      offset: number;
      limit: number;
    }) => {
      const url = `${API_BASE}${buildUrl(api.topology.get.path, {
        id: clusterId,
      })}?topic_offset=${offset}&topic_limit=${limit}`;
      const res = await fetch(url);
      if (!res.ok) throw new Error("Failed to load more topics");
      return api.topology.get.responses[200].parse(await res.json());
    },
  });
}

export function useRefreshTopology() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async (clusterId: number) => {
      const url = `${API_BASE}${buildUrl(api.topology.refresh.path, { id: clusterId })}`;
      const res = await fetch(url, {
        method: api.topology.refresh.method,
      });
      if (!res.ok) throw new Error("Failed to refresh topology");
      return res.json(); // full snapshot returned by server
    },
    onSuccess: (_data, clusterId) => {
      // Invalidate topology query so it re-fetches page 1 with pagination
      queryClient.invalidateQueries({ queryKey: [api.topology.get.path, clusterId] });
    },
  });
}

// ============================================
// TOPOLOGY SEARCH (across ALL topics in the full snapshot, not just loaded ones)
// ============================================

/** Search the full topology snapshot on the server. Returns matching nodes + edges + matchIds. */
export async function searchTopologyOnServer(
  clusterId: number,
  query: string
): Promise<{ nodes: any[]; edges: any[]; matchIds: string[] }> {
  const url = `${API_BASE}/api/clusters/${clusterId}/topology/search?q=${encodeURIComponent(query)}`;
  const res = await fetch(url);
  if (!res.ok) return { nodes: [], edges: [], matchIds: [] };
  return res.json();
}

// ============================================
// AI QUERY
// ============================================

export function useAiStatus() {
  return useQuery({
    queryKey: ["/api/ai/status"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/ai/status`);
      if (!res.ok) return { provider: null, configured: false, model: null };
      return res.json() as Promise<{ provider: string | null; configured: boolean; model: string | null }>;
    },
    staleTime: 5 * 60 * 1000, // cache for 5 minutes
  });
}

export function useAiQuery() {
  return useMutation({
    mutationFn: async (data: { question: string; topology: any }) => {
      const res = await fetch(`${API_BASE}${api.ai.query.path}`, {
        method: api.ai.query.method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error("AI Query failed");
      return api.ai.query.responses[200].parse(await res.json());
    },
  });
}
