import { render, screen } from "@testing-library/react";
import { StreamsEdge } from "./StreamsEdge";

vi.mock("reactflow", () => ({
  getBezierPath: vi.fn(() => ["M0,0 L100,100", 50, 50]),
  BaseEdge: ({ path, style }: any) => (
    <div data-testid="base-edge">
      <div data-testid="edge-path" data-path={path} data-style={JSON.stringify(style)} />
    </div>
  ),
}));

const defaultEdgeProps = {
  id: "e1-2",
  source: "topic:input",
  target: "topic:output",
  sourceX: 0,
  sourceY: 0,
  targetX: 100,
  targetY: 100,
  sourcePosition: { x: 0, y: 0 } as any,
  targetPosition: { x: 100, y: 100 } as any,
  data: {},
} as any;

describe("StreamsEdge", () => {
  it("renders with required ReactFlow edge props", () => {
    const { container } = render(<StreamsEdge {...defaultEdgeProps} />);
    expect(container.querySelector('[data-testid="base-edge"]')).toBeInTheDocument();
  });

  it("renders the edge path", () => {
    const { container } = render(<StreamsEdge {...defaultEdgeProps} />);
    const path = container.querySelector('[data-testid="edge-path"]');
    expect(path).toBeInTheDocument();
    expect(path).toHaveAttribute("data-path");
  });

  it("passes stroke style including strokeDasharray to BaseEdge", () => {
    const { container } = render(<StreamsEdge {...defaultEdgeProps} />);
    const path = container.querySelector('[data-testid="edge-path"]');
    const styleStr = path?.getAttribute("data-style") ?? "";
    expect(styleStr).toContain("8 4");
  });

  it("accepts label prop without crashing", () => {
    const { container } = render(
      <StreamsEdge {...defaultEdgeProps} label="my-app" />
    );
    expect(container.querySelector('[data-testid="base-edge"]')).toBeInTheDocument();
  });
});
