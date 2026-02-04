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
// TOPOLOGY
// ============================================

export function useTopology(clusterId: number) {
  return useQuery({
    queryKey: [api.topology.get.path, clusterId],
    queryFn: async () => {
      const url = `${API_BASE}${buildUrl(api.topology.get.path, { id: clusterId })}`;
      const res = await fetch(url);
      if (res.status === 404) return null; // No snapshot yet
      if (!res.ok) throw new Error("Failed to fetch topology");
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
      return api.topology.refresh.responses[200].parse(await res.json());
    },
    onSuccess: (snapshot, clusterId) => {
      queryClient.setQueryData([api.topology.get.path, clusterId], snapshot);
    },
  });
}

// ============================================
// REGISTRATIONS (optional producers/consumers, no interceptors)
// ============================================

export type Registration = { id?: number; clusterId?: number; appName: string; role: string; topics: string[]; outputTopics?: string[] };

export function useRegistrations(clusterId: number) {
  return useQuery({
    queryKey: [api.registrations.list.path, clusterId],
    queryFn: async () => {
      const url = `${API_BASE}${buildUrl(api.registrations.list.path, { id: clusterId })}`;
      const res = await fetch(url);
      if (!res.ok) throw new Error("Failed to fetch registrations");
      return (await res.json()) as Registration[];
    },
  });
}

export function useUpsertRegistration(clusterId: number) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async (data: { appName: string; role: string; topics: string[]; outputTopics?: string[] }) => {
      const url = `${API_BASE}${buildUrl(api.registrations.upsert.path, { id: clusterId })}`;
      const body: Record<string, unknown> = { appName: data.appName, role: data.role, topics: data.topics };
      if (data.outputTopics != null) body.outputTopics = data.outputTopics;
      const res = await fetch(url, {
        method: api.registrations.upsert.method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) throw new Error("Failed to register app");
      return (await res.json()) as { appName: string; role: string; topics: string[] };
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.registrations.list.path, clusterId] });
      // Do NOT invalidate topology here: it would trigger GET /topology which returns
      // the old cached snapshot and overwrites the cache. Caller must trigger refresh
      // and refresh's onSuccess will setQueryData with the new snapshot.
    },
  });
}

export function useDeleteRegistration(clusterId: number) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async (appName: string) => {
      const path = buildUrl(api.registrations.delete.path, { id: clusterId, appName: encodeURIComponent(appName) });
      const url = `${API_BASE}${path}`;
      const res = await fetch(url, { method: api.registrations.delete.method });
      if (!res.ok) throw new Error("Failed to remove registration");
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.registrations.list.path, clusterId] });
      // Do NOT invalidate topology; caller triggers refresh which setQueryData.
    },
  });
}

// ============================================
// AI QUERY
// ============================================

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
