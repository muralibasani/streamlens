import { render, screen } from "@testing-library/react";
import Dashboard from "./Dashboard";
import { useClusters, useClusterHealth } from "@/hooks/use-kafka";

vi.mock("@/hooks/use-kafka", () => ({
  useClusters: vi.fn(),
  useClusterHealth: vi.fn(),
  useCreateCluster: vi.fn(),
  useDeleteCluster: vi.fn(),
}));

vi.mock("wouter", () => ({
  Link: ({ children, href, ...props }: any) => (
    <a href={href} {...props}>
      {children}
    </a>
  ),
}));

vi.mock("@/components/ThemeToggle", () => ({
  ThemeToggle: () => <div data-testid="theme-toggle">ThemeToggle</div>,
}));

const mockUseClusters = vi.mocked(useClusters);
const mockUseClusterHealth = vi.mocked(useClusterHealth);

describe("Dashboard", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockUseClusterHealth.mockReturnValue({
      data: { online: true, clusterMode: "kraft" },
      isLoading: false,
      error: null,
    } as any);
  });

  it("shows loading state when isLoading is true", () => {
    mockUseClusters.mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
    } as any);

    render(<Dashboard />);
    expect(screen.getByText("Loading clusters...")).toBeInTheDocument();
  });

  it("shows 'No clusters configured' when clusters list is empty", () => {
    mockUseClusters.mockReturnValue({
      data: [],
      isLoading: false,
      error: null,
    } as any);

    render(<Dashboard />);
    expect(screen.getByText("No clusters configured")).toBeInTheDocument();
    expect(screen.getByText("server/data/clusters.json")).toBeInTheDocument();
  });

  it("shows 'No clusters configured' when error is present", () => {
    mockUseClusters.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error("Failed to fetch"),
    } as any);

    render(<Dashboard />);
    expect(screen.getByText("No clusters configured")).toBeInTheDocument();
  });

  it("shows cluster cards when clusters exist", () => {
    mockUseClusters.mockReturnValue({
      data: [
        {
          id: 1,
          name: "My Cluster",
          bootstrapServers: "localhost:9092",
          createdAt: new Date().toISOString(),
        },
      ],
      isLoading: false,
      error: null,
    } as any);

    render(<Dashboard />);
    expect(screen.getByText("My Cluster")).toBeInTheDocument();
    expect(screen.getByText("localhost:9092")).toBeInTheDocument();
    expect(screen.getByText("View Topology")).toBeInTheDocument();
  });

  it("shows 'Your clusters' heading and count when clusters exist", () => {
    mockUseClusters.mockReturnValue({
      data: [
        {
          id: 1,
          name: "Cluster A",
          bootstrapServers: "localhost:9092",
          createdAt: new Date().toISOString(),
        },
      ],
      isLoading: false,
      error: null,
    } as any);

    render(<Dashboard />);
    expect(screen.getByText("Your clusters")).toBeInTheDocument();
    expect(screen.getByText(/1 cluster/)).toBeInTheDocument();
  });

  it("shows multiple cluster cards when multiple clusters exist", () => {
    mockUseClusters.mockReturnValue({
      data: [
        {
          id: 1,
          name: "Cluster A",
          bootstrapServers: "localhost:9092",
          createdAt: new Date().toISOString(),
        },
        {
          id: 2,
          name: "Cluster B",
          bootstrapServers: "broker:9092",
          createdAt: new Date().toISOString(),
        },
      ],
      isLoading: false,
      error: null,
    } as any);

    render(<Dashboard />);
    expect(screen.getByText("Cluster A")).toBeInTheDocument();
    expect(screen.getByText("Cluster B")).toBeInTheDocument();
    expect(screen.getAllByText("View Topology")).toHaveLength(2);
  });
});
