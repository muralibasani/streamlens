import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { AiChatPanel } from "./AiChatPanel";
import { useAiQuery, useAiStatus } from "@/hooks/use-kafka";

vi.mock("@/hooks/use-kafka", () => ({
  useAiQuery: vi.fn(),
  useAiStatus: vi.fn(),
}));

vi.mock("wouter", () => ({
  useRoute: () => [true, { id: "1" }],
  useLocation: () => ["/topology/1", vi.fn()],
  Link: ({ children, ...props }: any) => <a {...props}>{children}</a>,
  Route: ({ children }: any) => <>{children}</>,
}));

const mockUseAiQuery = vi.mocked(useAiQuery);
const mockUseAiStatus = vi.mocked(useAiStatus);

describe("AiChatPanel", () => {
  const defaultProps = {
    topology: { data: { nodes: [], edges: [] } },
    onHighlightNodes: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();
    mockUseAiQuery.mockReturnValue({
      mutateAsync: vi.fn(),
      isPending: false,
    } as any);
    mockUseAiStatus.mockReturnValue({
      data: { configured: true, provider: "openai", model: "gpt-4" },
    } as any);
  });

  it("renders input field and send button", () => {
    render(<AiChatPanel {...defaultProps} />);
    expect(
      screen.getByPlaceholderText("Ask about your Kafka topology...")
    ).toBeInTheDocument();
    const sendButton = screen.getByRole("button");
    expect(sendButton).toBeInTheDocument();
  });

  it("renders StreamPilot header", () => {
    render(<AiChatPanel {...defaultProps} />);
    expect(screen.getByText("StreamPilot")).toBeInTheDocument();
  });

  it("shows 'Not configured' when AI is not available", () => {
    mockUseAiStatus.mockReturnValue({
      data: { configured: false, provider: null, model: null },
    } as any);

    render(<AiChatPanel {...defaultProps} />);
    expect(screen.getByText("Not configured")).toBeInTheDocument();
  });

  it("shows provider badge when AI is configured", () => {
    mockUseAiStatus.mockReturnValue({
      data: { configured: true, provider: "openai", model: "gpt-4" },
    } as any);

    render(<AiChatPanel {...defaultProps} />);
    expect(screen.getByText(/OpenAI/)).toBeInTheDocument();
  });

  it("renders welcome message from assistant", () => {
    render(<AiChatPanel {...defaultProps} />);
    expect(screen.getByText(/Hi! I'm StreamPilot/)).toBeInTheDocument();
  });

  it("disables send when input is empty", () => {
    render(<AiChatPanel {...defaultProps} />);
    const input = screen.getByPlaceholderText(
      "Ask about your Kafka topology..."
    );
    const form = input.closest("form");
    const submitButton = form?.querySelector('button[type="submit"]');
    expect(submitButton).toBeDisabled();
  });

  it("enables send when input has content", async () => {
    render(<AiChatPanel {...defaultProps} />);
    const input = screen.getByPlaceholderText(
      "Ask about your Kafka topology..."
    );
    fireEvent.change(input, { target: { value: "Which producers write to orders?" } });
    const submitButton = screen.getByRole("button");
    expect(submitButton).not.toBeDisabled();
  });

  it("shows loading state when AI query is pending", () => {
    mockUseAiQuery.mockReturnValue({
      mutateAsync: vi.fn(),
      isPending: true,
    } as any);

    render(<AiChatPanel {...defaultProps} />);
    expect(screen.getByText("Analyzing topology...")).toBeInTheDocument();
  });

  describe("submit handler", () => {
    it("calls mutateAsync with question and topology when submitting", async () => {
      const mutateAsync = vi.fn().mockResolvedValue({
        answer: "Here are the producers that write to orders.",
        highlightNodes: ["p1", "p2"],
      });

      mockUseAiQuery.mockReturnValue({
        mutateAsync,
        isPending: false,
      } as any);

      render(<AiChatPanel {...defaultProps} />);

      const input = screen.getByPlaceholderText("Ask about your Kafka topology...");
      fireEvent.change(input, { target: { value: "Which producers write to orders?" } });
      fireEvent.submit(input.closest("form")!);

      await waitFor(() => {
        expect(mutateAsync).toHaveBeenCalledWith({
          question: "Which producers write to orders?",
          topology: { nodes: [], edges: [] },
        });
      });
    });

    it("shows user message in chat after submit", async () => {
      mockUseAiQuery.mockReturnValue({
        mutateAsync: vi.fn().mockResolvedValue({
          answer: "AI response",
          highlightNodes: [],
        }),
        isPending: false,
      } as any);

      render(<AiChatPanel {...defaultProps} />);

      const input = screen.getByPlaceholderText("Ask about your Kafka topology...");
      fireEvent.change(input, { target: { value: "Show me consumers of orders" } });
      fireEvent.submit(input.closest("form")!);

      await waitFor(() => {
        expect(screen.getByText("Show me consumers of orders")).toBeInTheDocument();
      });
    });

    it("shows AI response in chat after mutateAsync resolves", async () => {
      mockUseAiQuery.mockReturnValue({
        mutateAsync: vi.fn().mockResolvedValue({
          answer: "The orders topic has 3 consumers: cg1, cg2, cg3.",
          highlightNodes: [],
        }),
        isPending: false,
      } as any);

      render(<AiChatPanel {...defaultProps} />);

      const input = screen.getByPlaceholderText("Ask about your Kafka topology...");
      fireEvent.change(input, { target: { value: "Who consumes orders?" } });
      fireEvent.submit(input.closest("form")!);

      await waitFor(() => {
        expect(screen.getByText(/The orders topic has 3 consumers/)).toBeInTheDocument();
      });
    });

    it("calls onHighlightNodes when AI returns highlightNodes", async () => {
      const onHighlightNodes = vi.fn();
      mockUseAiQuery.mockReturnValue({
        mutateAsync: vi.fn().mockResolvedValue({
          answer: "Here are the nodes.",
          highlightNodes: ["topic:orders", "consumer:cg1"],
        }),
        isPending: false,
      } as any);

      render(<AiChatPanel {...defaultProps} onHighlightNodes={onHighlightNodes} />);

      const input = screen.getByPlaceholderText("Ask about your Kafka topology...");
      fireEvent.change(input, { target: { value: "Show orders topology" } });
      fireEvent.submit(input.closest("form")!);

      await waitFor(() => {
        expect(onHighlightNodes).toHaveBeenCalledWith(["topic:orders", "consumer:cg1"]);
      });
    });

    it("shows error message when mutateAsync throws", async () => {
      mockUseAiQuery.mockReturnValue({
        mutateAsync: vi.fn().mockRejectedValue(new Error("Network error")),
        isPending: false,
      } as any);

      render(<AiChatPanel {...defaultProps} />);

      const input = screen.getByPlaceholderText("Ask about your Kafka topology...");
      fireEvent.change(input, { target: { value: "Something" } });
      fireEvent.submit(input.closest("form")!);

      await waitFor(() => {
        expect(screen.getByText(/Sorry, I encountered an error/)).toBeInTheDocument();
      });
    });

    it("clears input after submit", async () => {
      mockUseAiQuery.mockReturnValue({
        mutateAsync: vi.fn().mockResolvedValue({
          answer: "Done",
          highlightNodes: [],
        }),
        isPending: false,
      } as any);

      render(<AiChatPanel {...defaultProps} />);

      const input = screen.getByPlaceholderText("Ask about your Kafka topology...") as HTMLInputElement;
      fireEvent.change(input, { target: { value: "Hello" } });
      fireEvent.submit(input.closest("form")!);

      await waitFor(() => {
        expect(input.value).toBe("");
      });
    });

    it("scrolls message list on new message", async () => {
      const scrollIntoViewMock = vi.fn();
      Element.prototype.scrollIntoView = scrollIntoViewMock;

      mockUseAiQuery.mockReturnValue({
        mutateAsync: vi.fn().mockResolvedValue({
          answer: "New AI message",
          highlightNodes: [],
        }),
        isPending: false,
      } as any);

      render(<AiChatPanel {...defaultProps} />);

      const input = screen.getByPlaceholderText("Ask about your Kafka topology...");
      fireEvent.change(input, { target: { value: "Test scroll" } });
      fireEvent.submit(input.closest("form")!);

      await waitFor(() => {
        expect(screen.getByText("New AI message")).toBeInTheDocument();
      });

      expect(scrollIntoViewMock).toHaveBeenCalled();
    });
  });
});
