import { renderHook, act } from "@testing-library/react";
import { useTheme } from "@/hooks/use-theme";

const storage: Record<string, string> = {};
const mockLocalStorage = {
  getItem: (key: string) => storage[key] ?? null,
  setItem: (key: string, value: string) => {
    storage[key] = value;
  },
  removeItem: (key: string) => {
    delete storage[key];
  },
  clear: () => {
    Object.keys(storage).forEach((k) => delete storage[k]);
  },
  key: () => null,
  length: 0,
};

beforeEach(() => {
  Object.defineProperty(window, "localStorage", {
    value: mockLocalStorage,
    writable: true,
  });
  mockLocalStorage.removeItem("streamlens-theme");
  document.documentElement.classList.remove("dark");
});

describe("useTheme", () => {
  it("defaults to dark theme", () => {
    const { result } = renderHook(() => useTheme());
    expect(result.current.theme).toBe("dark");
  });

  it("reads stored theme from localStorage", () => {
    localStorage.setItem("streamlens-theme", "light");
    const { result } = renderHook(() => useTheme());
    expect(result.current.theme).toBe("light");
  });

  it("toggleTheme switches dark to light", () => {
    const { result } = renderHook(() => useTheme());
    act(() => {
      result.current.toggleTheme();
    });
    expect(result.current.theme).toBe("light");
    expect(localStorage.getItem("streamlens-theme")).toBe("light");
  });

  it("toggleTheme switches light to dark", () => {
    localStorage.setItem("streamlens-theme", "light");
    const { result } = renderHook(() => useTheme());
    act(() => {
      result.current.toggleTheme();
    });
    expect(result.current.theme).toBe("dark");
  });

  it("setTheme sets specific theme", () => {
    const { result } = renderHook(() => useTheme());
    act(() => {
      result.current.setTheme("light");
    });
    expect(result.current.theme).toBe("light");
  });

  it("applies dark class to documentElement", () => {
    renderHook(() => useTheme());
    expect(document.documentElement.classList.contains("dark")).toBe(true);
  });

  it("removes dark class for light theme", () => {
    localStorage.setItem("streamlens-theme", "light");
    renderHook(() => useTheme());
    expect(document.documentElement.classList.contains("dark")).toBe(false);
  });

  it("handles invalid stored value by defaulting to dark", () => {
    localStorage.setItem("streamlens-theme", "invalid");
    const { result } = renderHook(() => useTheme());
    expect(result.current.theme).toBe("dark");
  });
});
