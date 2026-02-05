import { z } from "zod";

// Always use relative paths so Vite proxy (dev) or same-origin (prod) handles /api — no CORS, no 403
export const API_BASE = "";

// Form / request types
export const insertClusterSchema = z.object({
  name: z.string().min(1, "Name is required"),
  bootstrapServers: z.string().min(1, "Bootstrap servers are required"),
  schemaRegistryUrl: z.string().optional(),
  connectUrl: z.string().optional(),
  jmxHost: z.string().optional(),
  jmxPort: z.preprocess(
    (v) => {
      if (v === "" || v === undefined || (typeof v === "number" && Number.isNaN(v))) return undefined;
      const n = Number(v);
      return Number.isNaN(n) ? undefined : n;
    },
    z.number().int().positive().optional()
  ),
});

export type InsertCluster = z.infer<typeof insertClusterSchema>;

// API response shapes (for parsing fetch responses)
const clusterSchema = z.object({
  id: z.number(),
  name: z.string(),
  bootstrapServers: z.string(),
  schemaRegistryUrl: z.string().nullable(),
  connectUrl: z.string().nullable(),
  jmxHost: z.string().nullable().optional(),
  jmxPort: z.number().nullable().optional(),
  enableKafkaEventProduceFromUi: z.boolean().optional(),
  createdAt: z.string().optional(),
});

const snapshotSchema = z.object({
  id: z.number(),
  clusterId: z.number(),
  data: z.object({
    nodes: z.array(z.any()),
    edges: z.array(z.any()),
  }),
  createdAt: z.string().optional(),
});

const aiQueryResponseSchema = z.object({
  answer: z.string(),
  highlightNodes: z.array(z.string()),
});

const validationErrorSchema = z.object({
  message: z.string(),
  field: z.string().optional(),
});

// API path constants and response parsers
export const api = {
  clusters: {
    list: {
      method: "GET" as const,
      path: "/api/clusters",
      responses: {
        200: z.array(clusterSchema),
      },
    },
    get: {
      method: "GET" as const,
      path: "/api/clusters/:id",
      responses: {
        200: clusterSchema,
        404: z.object({ message: z.string() }),
      },
    },
    health: {
      method: "GET" as const,
      path: "/api/clusters/:id/health",
      responses: {
        200: z.object({
          clusterId: z.number(),
          online: z.boolean(),
          error: z.string().nullable(),
          clusterMode: z.enum(["kraft", "zookeeper"]).nullable().optional(),
        }),
        404: z.object({ message: z.string() }),
      },
    },
    create: {
      method: "POST" as const,
      path: "/api/clusters",
      input: insertClusterSchema,
      responses: {
        201: clusterSchema,
        400: validationErrorSchema,
      },
    },
    update: {
      method: "PUT" as const,
      path: "/api/clusters/:id",
      input: insertClusterSchema,
      responses: {
        200: clusterSchema,
        404: z.object({ message: z.string() }),
      },
    },
    delete: {
      method: "DELETE" as const,
      path: "/api/clusters/:id",
      responses: {
        204: z.void(),
        404: z.object({ message: z.string() }),
      },
    },
  },
  topology: {
    get: {
      method: "GET" as const,
      path: "/api/clusters/:id/topology",
      responses: {
        200: snapshotSchema,
        404: z.object({ message: z.string() }),
      },
    },
    refresh: {
      method: "POST" as const,
      path: "/api/clusters/:id/refresh",
      responses: {
        200: snapshotSchema,
        404: z.object({ message: z.string() }),
      },
    },
  },
  ai: {
    query: {
      method: "POST" as const,
      path: "/api/ai/query",
      input: z.object({
        question: z.string(),
        topology: z.any(),
      }),
      responses: {
        200: aiQueryResponseSchema,
      },
    },
  },
};

export function buildUrl(path: string, params?: Record<string, string | number>): string {
  let url = path;
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (url.includes(`:${key}`)) {
        url = url.replace(`:${key}`, String(value));
      }
    }
  }
  return url;
}
