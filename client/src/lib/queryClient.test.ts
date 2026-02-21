import { apiRequest, getQueryFn } from "./queryClient";

describe("getQueryFn", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("returns parsed JSON on successful response", async () => {
    const mockData = { id: 1, name: "test" };
    const mockResponse = new Response(JSON.stringify(mockData), { status: 200 });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(mockResponse);

    const queryFn = getQueryFn({ on401: "throw" });
    const result = await queryFn({ queryKey: ["/api/data"] } as any);

    expect(result).toEqual(mockData);
    expect(fetch).toHaveBeenCalledWith("/api/data", { credentials: "include" });
  });

  it("throws on 401 when on401 is throw", async () => {
    const mockResponse = new Response("Unauthorized", {
      status: 401,
      statusText: "Unauthorized",
    });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(mockResponse);

    const queryFn = getQueryFn({ on401: "throw" });

    await expect(queryFn({ queryKey: ["/api/protected"] } as any)).rejects.toThrow(
      "401",
    );
  });

  it("throws on 404 response", async () => {
    const mockResponse = new Response("Not Found", {
      status: 404,
      statusText: "Not Found",
    });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(mockResponse);

    const queryFn = getQueryFn({ on401: "throw" });

    await expect(queryFn({ queryKey: ["/api/missing"] } as any)).rejects.toThrow(
      "404",
    );
  });
});

describe("apiRequest", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("makes a GET request and returns response on success", async () => {
    const mockResponse = new Response(JSON.stringify({ ok: true }), { status: 200 });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(mockResponse);

    const res = await apiRequest("GET", "/api/test");
    expect(res.ok).toBe(true);
    expect(fetch).toHaveBeenCalledWith("/api/test", {
      method: "GET",
      headers: {},
      body: undefined,
      credentials: "include",
    });
  });

  it("sends JSON body for POST requests", async () => {
    const mockResponse = new Response("{}", { status: 200 });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(mockResponse);

    await apiRequest("POST", "/api/test", { name: "hello" });
    expect(fetch).toHaveBeenCalledWith("/api/test", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "hello" }),
      credentials: "include",
    });
  });

  it("throws on non-ok response", async () => {
    const mockResponse = new Response("Not Found", { status: 404, statusText: "Not Found" });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(mockResponse);

    await expect(apiRequest("GET", "/api/missing")).rejects.toThrow("404");
  });
});
