import { render, screen, fireEvent, waitFor, within, act } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import Topology from "./Topology";
import {
  useTopology,
  useRefreshTopology,
  useLoadMoreTopics,
  useCluster,
  useClusterHealth,
  searchTopologyOnServer,
} from "@/hooks/use-kafka";

// --- Mocks ---

const mockFitView = vi.fn();
const mockSetCenter = vi.fn();

vi.mock("wouter", () => ({
  useRoute: () => [true, { id: "1" }],
  useLocation: () => ["/topology/1", vi.fn()],
  Link: ({ children, href, ...props }: any) => (
    <a href={href} {...props}>
      {children}
    </a>
  ),
}));

vi.mock("@/hooks/use-kafka", () => ({
  useTopology: vi.fn(),
  useRefreshTopology: vi.fn(),
  useLoadMoreTopics: vi.fn(),
  useCluster: vi.fn(),
  useClusterHealth: vi.fn(),
  searchTopologyOnServer: vi.fn(),
  TOPICS_PER_PAGE: 50,
}));

vi.mock("@/hooks/use-toast", () => ({
  useToast: () => ({ toast: vi.fn() }),
}));

vi.mock("@/hooks/use-theme", () => ({
  useTheme: () => ({ theme: "light" }),
}));

vi.mock("@/components/ThemeToggle", () => ({
  ThemeToggle: () => <div data-testid="theme-toggle">ThemeToggle</div>,
}));

vi.mock("@/components/AiChatPanel", () => ({
  AiChatPanel: () => <div data-testid="ai-chat-panel">AiChat</div>,
}));

// Mock ReactFlow — renders nodes as divs to inspect graph content.
// useNodesState/useEdgesState use real React.useState so state updates trigger re-renders.
vi.mock("reactflow", () => {
  const React = require("react");
  const ReactFlow = ({ nodes, edges, children }: any) => (
    <div data-testid="reactflow">
      <div data-testid="rf-nodes">
        {(nodes || []).map((n: any) => (
          <div
            key={n.id}
            data-testid={`node-${n.id}`}
            data-highlighted={n.data?.searchHighlighted ? "true" : "false"}
            data-type={n.data?.type}
          >
            {n.data?.label}
          </div>
        ))}
      </div>
      <div data-testid="rf-edges">
        {(edges || []).map((e: any) => (
          <div key={e.id} data-testid={`edge-${e.id}`} />
        ))}
      </div>
      {children}
    </div>
  );
  return {
    __esModule: true,
    default: ReactFlow,
    Background: () => null,
    Controls: () => null,
    useNodesState: (init: any[]) => {
      const [nodes, setNodes] = React.useState(init);
      return [nodes, setNodes, vi.fn()];
    },
    useEdgesState: (init: any[]) => {
      const [edges, setEdges] = React.useState(init);
      return [edges, setEdges, vi.fn()];
    },
    MarkerType: { ArrowClosed: "arrowclosed" },
    ReactFlowProvider: ({ children }: any) => <div>{children}</div>,
    useReactFlow: () => ({
      fitView: mockFitView,
      setCenter: mockSetCenter,
    }),
  };
});

vi.mock("dagre", () => {
  function GraphCtor() {
    return {
      setDefaultEdgeLabel: function () { return this; },
      setGraph: vi.fn(),
      setNode: vi.fn(),
      setEdge: vi.fn(),
      node: () => ({ x: 0, y: 0 }),
    };
  }
  (GraphCtor as any).prototype = {};
  return {
    __esModule: true,
    default: {
      graphlib: { Graph: GraphCtor },
      layout: vi.fn(),
    },
  };
});

// --- Test data ---

const makeTopologySnapshot = () => ({
  id: 1,
  data: {
    nodes: [
      { id: "topic:orders", type: "topic", data: { label: "orders", type: "topic" } },
      { id: "topic:payments", type: "topic", data: { label: "payments", type: "topic" } },
      { id: "topic:logs", type: "topic", data: { label: "logs", type: "topic" } },
      { id: "group:order-consumer", type: "consumer", data: { label: "order-consumer", type: "consumer" } },
      { id: "producer:pay-svc", type: "producer", data: { label: "pay-svc", type: "producer" } },
    ],
    edges: [
      { id: "e1", source: "topic:orders", target: "group:order-consumer" },
      { id: "e2", source: "producer:pay-svc", target: "topic:payments" },
    ],
  },
});

// --- Helpers ---

function setupMocks(overrides: Record<string, any> = {}) {
  vi.mocked(useTopology).mockReturnValue({
    data: overrides.snapshot ?? makeTopologySnapshot(),
    isLoading: false,
    refetch: vi.fn(),
  } as any);
  vi.mocked(useCluster).mockReturnValue({
    data: { id: 1, name: "Test Cluster", bootstrapServers: "localhost:9092" },
    isLoading: false,
  } as any);
  vi.mocked(useClusterHealth).mockReturnValue({
    data: { online: true },
    isLoading: false,
  } as any);
  vi.mocked(useRefreshTopology).mockReturnValue({
    mutateAsync: vi.fn(),
    isPending: false,
  } as any);
  vi.mocked(useLoadMoreTopics).mockReturnValue({
    mutateAsync: vi.fn(),
  } as any);
  vi.mocked(searchTopologyOnServer).mockResolvedValue({
    nodes: [],
    edges: [],
    matchIds: [],
  });
}

// Helper to get all rendered node testids
function getRenderedNodeIds(): string[] {
  const container = screen.getByTestId("rf-nodes");
  const nodeDivs = container.querySelectorAll("[data-testid^='node-']");
  return Array.from(nodeDivs).map((el) =>
    el.getAttribute("data-testid")!.replace("node-", "")
  );
}

function getRenderedEdgeIds(): string[] {
  const container = screen.getByTestId("rf-edges");
  const edgeDivs = container.querySelectorAll("[data-testid^='edge-']");
  return Array.from(edgeDivs).map((el) =>
    el.getAttribute("data-testid")!.replace("edge-", "")
  );
}

// --- Tests ---

describe("Topology page — basic rendering", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupMocks();
  });

  it("shows loading state when topology is loading", () => {
    vi.mocked(useTopology).mockReturnValue({
      data: undefined,
      isLoading: true,
      refetch: vi.fn(),
    } as any);
    render(<Topology />);
    expect(screen.queryByText("Cluster Overview")).not.toBeInTheDocument();
  });

  it("shows initial crawl button when no snapshot exists", () => {
    vi.mocked(useTopology).mockReturnValue({
      data: undefined,
      isLoading: false,
      refetch: vi.fn(),
    } as any);
    render(<Topology />);
    expect(screen.getByText("No Topology Data Yet")).toBeInTheDocument();
    expect(screen.getByText("Run Initial Crawl")).toBeInTheDocument();
  });

  it("renders all nodes from the topology snapshot", async () => {
    render(<Topology />);
    await waitFor(() => {
      const nodeIds = getRenderedNodeIds();
      expect(nodeIds).toContain("topic:orders");
      expect(nodeIds).toContain("topic:payments");
      expect(nodeIds).toContain("topic:logs");
      expect(nodeIds).toContain("group:order-consumer");
      expect(nodeIds).toContain("producer:pay-svc");
    });
  });

  it("renders edges from the topology snapshot", async () => {
    render(<Topology />);
    await waitFor(() => {
      const edgeIds = getRenderedEdgeIds();
      expect(edgeIds).toContain("e1");
      expect(edgeIds).toContain("e2");
    });
  });

  it("shows stats panel with entity counts", async () => {
    render(<Topology />);
    await waitFor(() => {
      expect(screen.getByText("Cluster Overview")).toBeInTheDocument();
      expect(screen.getByText("Topics")).toBeInTheDocument();
    });
  });
});

describe("Topology page — filter toggle button", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupMocks();
  });

  it("renders the filter toggle button", async () => {
    render(<Topology />);
    await waitFor(() => {
      expect(screen.getByTitle(/mode/i)).toBeInTheDocument();
    });
  });

  it("defaults to highlight mode", async () => {
    render(<Topology />);
    await waitFor(() => {
      expect(screen.getByTitle(/Highlight mode/i)).toBeInTheDocument();
    });
  });

  it("toggles to filter mode on click", async () => {
    render(<Topology />);
    await waitFor(() => screen.getByTitle(/Highlight mode/i));
    await userEvent.click(screen.getByTitle(/Highlight mode/i));
    expect(screen.getByTitle(/Filter mode/i)).toBeInTheDocument();
  });

  it("toggles back to highlight mode on second click", async () => {
    render(<Topology />);
    await waitFor(() => screen.getByTitle(/Highlight mode/i));
    await userEvent.click(screen.getByTitle(/Highlight mode/i));
    await userEvent.click(screen.getByTitle(/Filter mode/i));
    expect(screen.getByTitle(/Highlight mode/i)).toBeInTheDocument();
  });

  it("has blue styling when filter mode is active", async () => {
    render(<Topology />);
    await waitFor(() => screen.getByTitle(/Highlight mode/i));
    const btn = screen.getByTitle(/Highlight mode/i);
    // In highlight mode, should NOT have blue class
    expect(btn.className).not.toMatch(/bg-blue/);
    await userEvent.click(btn);
    const filterBtn = screen.getByTitle(/Filter mode/i);
    expect(filterBtn.className).toMatch(/bg-blue/);
  });
});

describe("Topology page — search in highlight mode", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupMocks();
  });

  it("renders search input", async () => {
    render(<Topology />);
    await waitFor(() => {
      expect(screen.getByPlaceholderText("Search nodes...")).toBeInTheDocument();
    });
  });

  it("highlights matching nodes without hiding others", async () => {
    render(<Topology />);
    await waitFor(() => expect(getRenderedNodeIds().length).toBe(5));

    const input = screen.getByPlaceholderText("Search nodes...");
    await userEvent.type(input, "orders");

    await waitFor(() => {
      // All 5 nodes should still be rendered (highlight mode doesn't hide)
      expect(getRenderedNodeIds().length).toBe(5);
      // The matching node should be highlighted
      const ordersNode = screen.getByTestId("node-topic:orders");
      expect(ordersNode.getAttribute("data-highlighted")).toBe("true");
    });
  });

  it("shows match count text in highlight mode", async () => {
    render(<Topology />);
    await waitFor(() => expect(getRenderedNodeIds().length).toBe(5));

    const input = screen.getByPlaceholderText("Search nodes...");
    await userEvent.type(input, "orders");

    await waitFor(() => {
      expect(screen.getByText(/match/i)).toBeInTheDocument();
    });
  });

  it("clears search and removes highlights", async () => {
    render(<Topology />);
    await waitFor(() => expect(getRenderedNodeIds().length).toBe(5));

    const input = screen.getByPlaceholderText("Search nodes...");
    await userEvent.type(input, "orders");

    await waitFor(() => {
      expect(screen.getByTestId("node-topic:orders").getAttribute("data-highlighted")).toBe("true");
    });

    // Clear by clicking X button (the clear button appears when search has text)
    // Find the X button — it's a <button> near the search input
    const clearBtns = screen.getAllByRole("button").filter(
      (btn) => btn.closest(".relative.w-64") !== null
    );
    // Click the last one (the X close button)
    const closeBtn = clearBtns[clearBtns.length - 1];
    await userEvent.click(closeBtn);

    await waitFor(() => {
      // All highlights should be cleared
      expect(getRenderedNodeIds().length).toBe(5);
      expect(screen.getByTestId("node-topic:orders").getAttribute("data-highlighted")).toBe("false");
    });
  });
});

describe("Topology page — search in filter mode", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupMocks();
  });

  it("hides unmatched nodes when filtering", async () => {
    render(<Topology />);
    await waitFor(() => expect(getRenderedNodeIds().length).toBe(5));

    // Switch to filter mode
    await userEvent.click(screen.getByTitle(/Highlight mode/i));
    expect(screen.getByTitle(/Filter mode/i)).toBeInTheDocument();

    // Type a query
    const input = screen.getByPlaceholderText("Search nodes...");
    await userEvent.type(input, "orders");

    await waitFor(() => {
      const nodeIds = getRenderedNodeIds();
      // "orders" matches topic:orders (label). Its neighbor is group:order-consumer.
      // topic:logs, topic:payments, producer:pay-svc should be hidden.
      expect(nodeIds).toContain("topic:orders");
      expect(nodeIds).toContain("group:order-consumer");
      expect(nodeIds).not.toContain("topic:logs");
      expect(nodeIds).not.toContain("producer:pay-svc");
    });
  });

  it("shows matching edges only between visible nodes", async () => {
    render(<Topology />);
    await waitFor(() => expect(getRenderedNodeIds().length).toBe(5));

    await userEvent.click(screen.getByTitle(/Highlight mode/i));
    const input = screen.getByPlaceholderText("Search nodes...");
    await userEvent.type(input, "orders");

    await waitFor(() => {
      const edgeIds = getRenderedEdgeIds();
      // e1 connects topic:orders → group:order-consumer (both visible)
      expect(edgeIds).toContain("e1");
      // e2 connects producer:pay-svc → topic:payments (neither visible in filtered view)
      expect(edgeIds).not.toContain("e2");
    });
  });

  it("shows filter status text with node counts", async () => {
    render(<Topology />);
    await waitFor(() => expect(getRenderedNodeIds().length).toBe(5));

    await userEvent.click(screen.getByTitle(/Highlight mode/i));
    const input = screen.getByPlaceholderText("Search nodes...");
    await userEvent.type(input, "orders");

    await waitFor(() => {
      // Filter mode shows "Showing X of Y nodes (Z matched)"
      expect(screen.getByText(/Showing.*of.*nodes/)).toBeInTheDocument();
    });
  });

  it("shows empty graph with 'No matches' for zero results", async () => {
    render(<Topology />);
    await waitFor(() => expect(getRenderedNodeIds().length).toBe(5));

    await userEvent.click(screen.getByTitle(/Highlight mode/i));
    const input = screen.getByPlaceholderText("Search nodes...");
    await userEvent.type(input, "zzzznonexistent");

    await waitFor(() => {
      expect(getRenderedNodeIds().length).toBe(0);
      expect(screen.getByText("No matches")).toBeInTheDocument();
    });
  });

  it("restores full graph when clearing filter", async () => {
    render(<Topology />);
    await waitFor(() => expect(getRenderedNodeIds().length).toBe(5));

    // Switch to filter mode and search
    await userEvent.click(screen.getByTitle(/Highlight mode/i));
    const input = screen.getByPlaceholderText("Search nodes...");
    await userEvent.type(input, "orders");

    await waitFor(() => {
      expect(getRenderedNodeIds().length).toBeLessThan(5);
    });

    // Clear search by clicking the X button
    const clearBtns = screen.getAllByRole("button").filter(
      (btn) => btn.closest(".relative.w-64") !== null
    );
    const closeBtn = clearBtns[clearBtns.length - 1];
    await userEvent.click(closeBtn);

    await waitFor(() => {
      // All 5 nodes should be restored
      expect(getRenderedNodeIds().length).toBe(5);
    });
  });

  it("restores full graph when clearing filter via empty input", async () => {
    render(<Topology />);
    await waitFor(() => expect(getRenderedNodeIds().length).toBe(5));

    await userEvent.click(screen.getByTitle(/Highlight mode/i));
    const input = screen.getByPlaceholderText("Search nodes...");
    await userEvent.type(input, "orders");

    await waitFor(() => {
      expect(getRenderedNodeIds().length).toBeLessThan(5);
    });

    // Clear by selecting all text and deleting
    await userEvent.clear(input);

    await waitFor(() => {
      expect(getRenderedNodeIds().length).toBe(5);
    });
  });
});

describe("Topology page — mode toggle mid-search", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupMocks();
  });

  it("switching highlight → filter mid-search hides unmatched nodes", async () => {
    render(<Topology />);
    await waitFor(() => expect(getRenderedNodeIds().length).toBe(5));

    // Search in highlight mode first
    const input = screen.getByPlaceholderText("Search nodes...");
    await userEvent.type(input, "orders");

    await waitFor(() => {
      // In highlight mode, all 5 nodes visible
      expect(getRenderedNodeIds().length).toBe(5);
    });

    // Now toggle to filter mode
    await userEvent.click(screen.getByTitle(/Highlight mode/i));

    await waitFor(() => {
      // Should now filter: only matched + neighbors visible
      const nodeIds = getRenderedNodeIds();
      expect(nodeIds).toContain("topic:orders");
      expect(nodeIds).not.toContain("topic:logs");
    });
  });

  it("switching filter → highlight mid-search restores all nodes with highlights", async () => {
    render(<Topology />);
    await waitFor(() => expect(getRenderedNodeIds().length).toBe(5));

    // Switch to filter mode and search
    await userEvent.click(screen.getByTitle(/Highlight mode/i));
    const input = screen.getByPlaceholderText("Search nodes...");
    await userEvent.type(input, "orders");

    await waitFor(() => {
      expect(getRenderedNodeIds().length).toBeLessThan(5);
    });

    // Toggle back to highlight mode
    await userEvent.click(screen.getByTitle(/Filter mode/i));

    await waitFor(() => {
      // All nodes should be visible again
      expect(getRenderedNodeIds().length).toBe(5);
      // Matching nodes should be highlighted
      expect(screen.getByTestId("node-topic:orders").getAttribute("data-highlighted")).toBe("true");
      // Non-matching nodes should not be highlighted
      expect(screen.getByTestId("node-topic:logs").getAttribute("data-highlighted")).toBe("false");
    });
  });
});

describe("Topology page — stats panel during filter", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupMocks();
  });

  it("shows X / Y format in stats panel when filter is active", async () => {
    render(<Topology />);
    await waitFor(() => expect(getRenderedNodeIds().length).toBe(5));

    // Switch to filter mode and search
    await userEvent.click(screen.getByTitle(/Highlight mode/i));
    const input = screen.getByPlaceholderText("Search nodes...");
    await userEvent.type(input, "orders");

    await waitFor(() => {
      // Stats panel should show "X / Y" format for topics
      // The snapshot has 3 topics total, filter shows fewer
      expect(screen.getByText(/\/ 3/)).toBeInTheDocument();
    });
  });

  it("reverts stats to plain counts after clearing filter", async () => {
    render(<Topology />);
    await waitFor(() => expect(getRenderedNodeIds().length).toBe(5));

    await userEvent.click(screen.getByTitle(/Highlight mode/i));
    const input = screen.getByPlaceholderText("Search nodes...");
    await userEvent.type(input, "orders");

    await waitFor(() => {
      expect(screen.getByText(/\/ 3/)).toBeInTheDocument();
    });

    // Clear filter
    await userEvent.clear(input);

    await waitFor(() => {
      // Should no longer show "/ 3" format
      expect(screen.queryByText(/\/ 3/)).not.toBeInTheDocument();
    });
  });
});
