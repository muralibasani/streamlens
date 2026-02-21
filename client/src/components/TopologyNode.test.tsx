import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { ReactFlowProvider } from "reactflow";
import TopologyNode from "./TopologyNode";

vi.mock("wouter", () => ({
  useRoute: () => [true, { id: "1" }],
}));

vi.mock("@/hooks/use-toast", () => ({
  useToast: () => ({ toast: vi.fn() }),
}));

const mockFetch = vi.fn();
(globalThis as any).fetch = mockFetch;

function renderNode(data: Record<string, unknown>, selected = false) {
  return render(
    <ReactFlowProvider>
      <TopologyNode data={data} selected={selected} />
    </ReactFlowProvider>
  );
}

function getClickableNode(label: string) {
  const node = screen.getByText(label);
  return node.closest('[class*="min-w"]');
}

describe("TopologyNode", () => {
  describe("topic node", () => {
    it("renders the topic label", () => {
      renderNode({ type: "topic", label: "orders" });
      expect(screen.getByText("orders")).toBeInTheDocument();
    });

    it("renders the TOPIC type label", () => {
      renderNode({ type: "topic", label: "orders" });
      expect(screen.getByText("Topic")).toBeInTheDocument();
    });
  });

  describe("consumer node", () => {
    it("renders the consumer label", () => {
      renderNode({ type: "consumer", label: "my-consumer-group", source: "auto-discovered" });
      expect(screen.getByText("my-consumer-group")).toBeInTheDocument();
    });

    it("shows Live badge for auto-discovered source", () => {
      renderNode({ type: "consumer", label: "cg", source: "auto-discovered" });
      expect(screen.getByText("Live")).toBeInTheDocument();
    });
  });

  describe("producer node", () => {
    it("renders the producer label", () => {
      renderNode({ type: "producer", label: "my-producer", source: "jmx" });
      expect(screen.getByText("my-producer")).toBeInTheDocument();
    });

    it("shows JMX badge", () => {
      renderNode({ type: "producer", label: "p", source: "jmx" });
      expect(screen.getByText("JMX")).toBeInTheDocument();
    });
  });

  describe("source badges", () => {
    it("shows Offset badge for offset source", () => {
      renderNode({ type: "consumer", label: "cg", source: "offset" });
      expect(screen.getByText("Offset")).toBeInTheDocument();
    });

    it("shows Config badge for config source", () => {
      renderNode({ type: "producer", label: "p", source: "config" });
      expect(screen.getByText("Config")).toBeInTheDocument();
    });

    it("shows ACL badge for acl source", () => {
      renderNode({ type: "producer", label: "p", source: "acl" });
      expect(screen.getByText("ACL")).toBeInTheDocument();
    });

    it("shows Manual badge for manual source", () => {
      renderNode({ type: "consumer", label: "cg", source: "manual" });
      expect(screen.getByText("Manual")).toBeInTheDocument();
    });
  });

  describe("schema node", () => {
    it("renders single subject as label", () => {
      renderNode({
        type: "schema",
        label: "orders-value",
        subLabel: "AVRO",
        schemaType: "AVRO",
        subjects: ["orders-value"],
        subject: "orders-value",
        version: 1,
      });
      expect(screen.getByText("orders-value")).toBeInTheDocument();
      expect(screen.getByText("AVRO")).toBeInTheDocument();
    });

    it("renders Multiple subjects for grouped schemas", () => {
      renderNode({
        type: "schema",
        label: "Multiple subjects",
        subLabel: "AVRO · 2 subject(s)",
        schemaType: "AVRO",
        subjects: ["orders-value", "payments-value"],
        subject: "orders-value",
        version: 1,
        schemaId: 42,
      });
      expect(screen.getByText("Multiple subjects")).toBeInTheDocument();
      expect(screen.getByText("AVRO · 2 subject(s)")).toBeInTheDocument();
    });

    it("renders the SCHEMA type label", () => {
      renderNode({
        type: "schema",
        label: "orders-value",
        subjects: ["orders-value"],
        subject: "orders-value",
        version: 1,
      });
      expect(screen.getByText("Schema")).toBeInTheDocument();
    });
  });

  describe("streams node", () => {
    it("renders streams node with correct icon and label", () => {
      renderNode({ type: "streams", label: "my-stream" });
      expect(screen.getByText("my-stream")).toBeInTheDocument();
      expect(screen.getByText("Streams")).toBeInTheDocument();
    });
  });

  describe("unknown node type", () => {
    it("renders unknown type node", () => {
      renderNode({ type: "unknown", label: "something" });
      expect(screen.getByText("something")).toBeInTheDocument();
    });
  });

  describe("subLabel", () => {
    it("renders subLabel when provided", () => {
      renderNode({ type: "topic", label: "orders", subLabel: "Partitions: 3" });
      expect(screen.getByText("Partitions: 3")).toBeInTheDocument();
    });
  });

  describe("source badge default", () => {
    it("shows no badge for unknown source", () => {
      renderNode({ type: "consumer", label: "cg", source: "unknown" });
      expect(screen.queryByText("Live")).not.toBeInTheDocument();
      expect(screen.queryByText("Config")).not.toBeInTheDocument();
      expect(screen.getByText("cg")).toBeInTheDocument();
    });
  });

  describe("connector node", () => {
    it("renders the connector label", () => {
      renderNode({ type: "connector", label: "my-sink-connector" });
      expect(screen.getByText("my-sink-connector")).toBeInTheDocument();
    });

    it("shows source badge when connector has source from config", () => {
      renderNode({ type: "connector", label: "db-sink", source: "config" });
      expect(screen.getByText("Config")).toBeInTheDocument();
    });
  });

  describe("acl node", () => {
    it("renders the ACL label", () => {
      renderNode({
        type: "acl",
        label: "ACL (3)",
        topic: "orders",
        acls: [
          { principal: "User:alice", host: "*", operation: "READ", permissionType: "ALLOW" },
          { principal: "User:bob", host: "*", operation: "WRITE", permissionType: "ALLOW" },
          { principal: "User:carol", host: "*", operation: "DESCRIBE", permissionType: "ALLOW" },
        ],
      });
      expect(screen.getByText("ACL (3)")).toBeInTheDocument();
    });

    it("renders ACL node with ACL type label", () => {
      renderNode({
        type: "acl",
        label: "ACL (2)",
        topic: "payments",
        acls: [
          { principal: "User:admin", host: "*", operation: "ALL", permissionType: "ALLOW" },
        ],
      });
      expect(screen.getByText("ACL")).toBeInTheDocument();
      expect(screen.getByText("ACL (2)")).toBeInTheDocument();
    });
  });

  describe("visual states", () => {
    it("applies highlight class when highlighted", () => {
      const { container } = renderNode({ type: "topic", label: "t", highlighted: true });
      const node = container.querySelector("[class*='scale-105']");
      expect(node).not.toBeNull();
    });

    it("applies search highlight class", () => {
      const { container } = renderNode({ type: "topic", label: "t", searchHighlighted: true });
      const node = container.querySelector("[class*='ring-yellow-500']");
      expect(node).not.toBeNull();
    });
  });

  describe("dialog flows", () => {
    beforeEach(() => {
      mockFetch.mockReset();
    });

    it("opens schema dialog on schema node click and fetches schema details", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          subject: "orders-value",
          version: 1,
          schema: "{}",
          schemaType: "AVRO",
          allVersions: [1],
        }),
      });

      renderNode({
        type: "schema",
        label: "orders-value",
        subject: "orders-value",
        schemaId: 10,
      });

      const clickable = getClickableNode("orders-value");
      fireEvent.click(clickable!);

      await waitFor(() => {
        expect(screen.getByText(/Schema:/)).toBeInTheDocument();
      });

      expect(mockFetch).toHaveBeenCalledWith(
        "/api/clusters/1/schema/orders-value"
      );
    });

    it("opens consumer lag dialog on consumer node click", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          topics: {
            orders: {
              partitions: [
                { partition: 0, currentOffset: 100, logEndOffset: 105, lag: 5 },
              ],
            },
          },
        }),
      });

      renderNode({
        type: "consumer",
        label: "my-consumer-group",
        details: { id: "my-consumer-group" },
      });

      const clickable = getClickableNode("my-consumer-group");
      fireEvent.click(clickable!);

      await waitFor(() => {
        expect(screen.getByText(/Consumer Lag:/)).toBeInTheDocument();
      });

      expect(mockFetch).toHaveBeenCalledWith(
        "/api/clusters/1/consumer/my-consumer-group/lag"
      );
    });

    it("opens topic dialog on topic node click and fetches topic details", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          name: "orders",
          partitions: 6,
          replicationFactor: 3,
          config: { cleanupPolicy: "delete", retentionMsDisplay: "7 days" },
        }),
      });

      renderNode({ type: "topic", label: "orders" });

      const clickable = getClickableNode("orders");
      fireEvent.click(clickable!);

      await waitFor(() => {
        expect(screen.getByText(/Topic:/)).toBeInTheDocument();
      });

      expect(mockFetch).toHaveBeenCalledWith(
        "/api/clusters/1/topic/orders/details"
      );
    });

    it("opens connector dialog on connector node click", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          name: "my-sink-connector",
          type: "sink",
          connectorClass: "io.confluent.connect.jdbc.JdbcSinkConnector",
          tasks: [{ connector: "my-sink-connector", task: 0 }],
          config: {},
        }),
      });

      renderNode({ type: "connector", label: "my-sink-connector" });

      const clickable = getClickableNode("my-sink-connector");
      fireEvent.click(clickable!);

      await waitFor(() => {
        expect(screen.getByText(/Connector:/)).toBeInTheDocument();
      });

      expect(mockFetch).toHaveBeenCalledWith(
        "/api/clusters/1/connector/my-sink-connector/details"
      );
    });

    it("opens ACL dialog on ACL node click", () => {
      renderNode({
        type: "acl",
        label: "ACL (2)",
        topic: "orders",
        acls: [
          { principal: "User:alice", host: "*", operation: "READ", permissionType: "ALLOW" },
          { principal: "User:bob", host: "*", operation: "WRITE", permissionType: "ALLOW" },
        ],
      });

      const clickable = getClickableNode("ACL (2)");
      fireEvent.click(clickable!);

      expect(screen.getByText(/ACL:/)).toBeInTheDocument();
      expect(screen.getByText(/2 binding\(s\)/)).toBeInTheDocument();
    });

    it("topic dialog shows produce section when enableProduceFromUi is true", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          name: "orders",
          partitions: 6,
          replicationFactor: 3,
          config: {},
        }),
      });

      renderNode({
        type: "topic",
        label: "orders",
        enableProduceFromUi: true,
        hasConnector: false,
      });

      const clickable = getClickableNode("orders");
      fireEvent.click(clickable!);

      await waitFor(() => {
        expect(screen.getByText(/Produce message/)).toBeInTheDocument();
      });
    });

    it("topic dialog shows Generate client code section", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          name: "orders",
          partitions: 6,
          replicationFactor: 3,
          config: {},
        }),
      });

      renderNode({ type: "topic", label: "orders" });

      const clickable = getClickableNode("orders");
      fireEvent.click(clickable!);

      await waitFor(() => {
        expect(screen.getByText(/Generate client code/)).toBeInTheDocument();
      });
    });

    it("schema dialog shows schema details after fetch", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          subject: "orders-value",
          version: 1,
          schema: '{"type":"record"}',
          schemaType: "AVRO",
          allVersions: [1],
        }),
      });

      renderNode({
        type: "schema",
        label: "orders-value",
        subject: "orders-value",
      });

      fireEvent.click(getClickableNode("orders-value")!);

      await waitFor(() => {
        expect(screen.getByText(/Subject/)).toBeInTheDocument();
        expect(screen.getByText(/Schema Definition/)).toBeInTheDocument();
        expect(screen.getByText(/Current Version/)).toBeInTheDocument();
      });
    });

    it("connector dialog shows connector details after fetch", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          name: "db-sink",
          type: "source",
          connectorClass: "io.confluent.connect.jdbc.JdbcSourceConnector",
          tasks: [{ connector: "db-sink", task: 0 }],
          config: { "connection.url": "jdbc:postgresql://localhost/db" },
        }),
      });

      renderNode({ type: "connector", label: "db-sink" });

      fireEvent.click(getClickableNode("db-sink")!);

      await waitFor(() => {
        expect(screen.getByText(/Connector Information/)).toBeInTheDocument();
        expect(screen.getByText(/Configuration/)).toBeInTheDocument();
        expect(screen.getByText(/jdbc:postgresql/)).toBeInTheDocument();
      });
    });

    it("topic dialog View Messages button loads messages", async () => {
      mockFetch
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            name: "orders",
            partitions: 6,
            replicationFactor: 3,
            config: {},
          }),
        })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            name: "orders",
            partitions: 6,
            replicationFactor: 3,
            config: {},
            recentMessages: [
              { partition: 0, offset: 42, key: "k1", value: "v1", timestamp: Date.now() },
            ],
          }),
        });

      renderNode({ type: "topic", label: "orders" });

      fireEvent.click(getClickableNode("orders")!);

      await waitFor(() => {
        expect(screen.getByRole("button", { name: /View Messages/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole("button", { name: /View Messages/i }));

      await waitFor(() => {
        expect(screen.getByText("v1")).toBeInTheDocument();
      });
    });

    it("topic dialog generate code fetches and displays code", async () => {
      mockFetch
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            name: "orders",
            partitions: 6,
            replicationFactor: 3,
            config: {},
          }),
        })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ code: "// Java producer code\nProducer<String,String> p = ..." }),
        });

      renderNode({ type: "topic", label: "orders" });

      fireEvent.click(getClickableNode("orders")!);

      await waitFor(() => {
        expect(screen.getByText(/Generate client code/)).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole("button", { name: /Generate/i }));

      await waitFor(() => {
        expect(screen.getByText(/Java producer code/)).toBeInTheDocument();
      });
    });

    it("schema dialog shows version selector and schema definition", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          subject: "orders-value",
          version: 2,
          id: 10,
          schema: '{"type":"record","name":"Order"}',
          schemaType: "AVRO",
          allVersions: [1, 2],
        }),
      });

      renderNode({
        type: "schema",
        label: "orders-value",
        subject: "orders-value",
      });

      fireEvent.click(getClickableNode("orders-value")!);

      await waitFor(() => {
        expect(screen.getByText(/Schema:/)).toBeInTheDocument();
      });

      await waitFor(() => {
        expect(screen.getByRole("button", { name: /v1/ })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: /v2/ })).toBeInTheDocument();
        expect(screen.getByText(/Schema Definition/)).toBeInTheDocument();
        expect(screen.getByText(/"name":\s*"Order"/)).toBeInTheDocument();
      });
    });

    it("schema dialog allows changing version", async () => {
      mockFetch
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            subject: "orders-value",
            version: 2,
            schema: "{}",
            schemaType: "AVRO",
            allVersions: [1, 2],
          }),
        })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            subject: "orders-value",
            version: 1,
            schema: "{}",
            schemaType: "AVRO",
            allVersions: [1, 2],
          }),
        });

      renderNode({
        type: "schema",
        label: "orders-value",
        subject: "orders-value",
      });

      fireEvent.click(getClickableNode("orders-value")!);

      await waitFor(() => {
        expect(screen.getByRole("button", { name: /v1/ })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole("button", { name: /^v1$/ }));

      await waitFor(() => {
        expect(mockFetch).toHaveBeenCalledWith(
          "/api/clusters/1/schema/orders-value?version=1"
        );
      });
    });

    it("schema dialog renders PROTOBUF schema as plain text", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          subject: "test-value",
          version: 1,
          schema: 'syntax = "proto3";',
          schemaType: "PROTOBUF",
          allVersions: [1],
        }),
      });

      renderNode({
        type: "schema",
        label: "test-value",
        subject: "test-value",
      });

      fireEvent.click(getClickableNode("test-value")!);

      await waitFor(() => {
        expect(screen.getByText(/syntax = "proto3";/)).toBeInTheDocument();
      });
    });

    it("consumer lag dialog shows partition lag data", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          topics: {
            orders: {
              partitions: [
                {
                  partition: 0,
                  currentOffset: 100,
                  logEndOffset: 150,
                  lag: 50,
                },
              ],
            },
          },
        }),
      });

      renderNode({
        type: "consumer",
        label: "my-consumer-group",
        details: { id: "my-consumer-group" },
      });

      fireEvent.click(getClickableNode("my-consumer-group")!);

      await waitFor(() => {
        expect(screen.getByText(/Consumer Lag:/)).toBeInTheDocument();
      });

      await waitFor(() => {
        expect(screen.getByText(/Partition/)).toBeInTheDocument();
        expect(screen.getByText(/Current Offset/)).toBeInTheDocument();
        expect(screen.getByText("100")).toBeInTheDocument();
        expect(screen.getByText("150")).toBeInTheDocument();
        expect(screen.getAllByText("50").length).toBeGreaterThanOrEqual(1);
      });
    });

    it("topic dialog shows produce form and handles produce", async () => {
      mockFetch
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            name: "orders",
            partitions: 1,
            replicationFactor: 1,
            config: { cleanupPolicy: "delete" },
            recentMessages: [],
          }),
        })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ ok: true, partition: 0, offset: 5 }),
        });

      renderNode({
        type: "topic",
        label: "orders",
        enableProduceFromUi: true,
        hasConnector: false,
      });

      fireEvent.click(getClickableNode("orders")!);

      await waitFor(() => {
        expect(screen.getByText(/Produce message/)).toBeInTheDocument();
      });

      const textarea = screen.getByPlaceholderText(/Enter message text/);
      fireEvent.change(textarea, { target: { value: "hello world" } });

      fireEvent.click(screen.getByRole("button", { name: /Produce/i }));

      await waitFor(() => {
        const postCalls = mockFetch.mock.calls.filter(
          (call) => (call[1] as RequestInit)?.method === "POST"
        );
        expect(postCalls.length).toBeGreaterThan(0);
        expect(postCalls[0][0]).toContain("/topic/orders/produce");
      });
    });

    it("topic dialog loads messages on button click", async () => {
      mockFetch
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            name: "orders",
            partitions: 1,
            replicationFactor: 1,
            config: {},
            recentMessages: [],
          }),
        })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            name: "orders",
            partitions: 1,
            replicationFactor: 1,
            config: {},
            recentMessages: [
              { partition: 0, offset: 1, key: "k1", value: "v1" },
            ],
          }),
        });

      renderNode({ type: "topic", label: "orders" });

      fireEvent.click(getClickableNode("orders")!);

      await waitFor(() => {
        expect(
          screen.getByRole("button", { name: /View Messages/i })
        ).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole("button", { name: /View Messages/i }));

      await waitFor(() => {
        expect(screen.getByText("v1")).toBeInTheDocument();
        expect(screen.getByText("k1")).toBeInTheDocument();
      });
    });

    it("copies generated code to clipboard", async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      Object.assign(navigator, { clipboard: { writeText } });

      mockFetch
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            name: "orders",
            partitions: 1,
            replicationFactor: 1,
            config: {},
          }),
        })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            code: "// Sample producer code\nProducer<String,String> p = ...",
          }),
        });

      renderNode({ type: "topic", label: "orders" });

      fireEvent.click(getClickableNode("orders")!);

      await waitFor(() => {
        expect(screen.getByText(/Generate client code/)).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole("button", { name: /Generate/i }));

      await waitFor(() => {
        expect(screen.getByText(/Sample producer code/)).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole("button", { name: /Copy/i }));

      await waitFor(() => {
        expect(writeText).toHaveBeenCalledWith(
          "// Sample producer code\nProducer<String,String> p = ..."
        );
      });
    });

    it("acl dialog shows bindings table", () => {
      renderNode({
        type: "acl",
        label: "ACL (1)",
        topic: "orders",
        acls: [
          {
            principal: "User:alice",
            host: "*",
            operation: "READ",
            permissionType: "ALLOW",
          },
        ],
      });

      fireEvent.click(getClickableNode("ACL (1)")!);

      expect(screen.getByText(/ACL:/)).toBeInTheDocument();
      expect(screen.getByText(/Principal/)).toBeInTheDocument();
      expect(screen.getByText(/Host/)).toBeInTheDocument();
      expect(screen.getByText(/Operation/)).toBeInTheDocument();
      expect(screen.getByText(/Permission/)).toBeInTheDocument();
      expect(screen.getByText("User:alice")).toBeInTheDocument();
      expect(screen.getByText("READ")).toBeInTheDocument();
      expect(screen.getByText("ALLOW")).toBeInTheDocument();
    });

    it("connector dialog shows connector info with connector class", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          name: "my-sink",
          type: "sink",
          connectorClass:
            "org.apache.kafka.connect.file.FileStreamSinkConnector",
          tasks: [{ connector: "my-sink", task: 0 }],
          config: {
            "connector.class":
              "org.apache.kafka.connect.file.FileStreamSinkConnector",
            topics: "orders",
          },
        }),
      });

      renderNode({ type: "connector", label: "my-sink" });

      fireEvent.click(getClickableNode("my-sink")!);

      await waitFor(() => {
        expect(screen.getByText(/Connector:/)).toBeInTheDocument();
        expect(
          screen.getAllByText(/FileStreamSinkConnector/).length
        ).toBeGreaterThanOrEqual(1);
      });
    });
  });
});
