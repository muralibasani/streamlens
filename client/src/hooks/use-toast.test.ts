import { renderHook, act } from "@testing-library/react";
import { reducer, useToast, toast } from "@/hooks/use-toast";

describe("toast reducer", () => {
  it("ADD_TOAST adds a toast", () => {
    const state = { toasts: [] };
    const result = reducer(state, {
      type: "ADD_TOAST",
      toast: { id: "1", title: "Test", open: true } as any,
    });
    expect(result.toasts).toHaveLength(1);
    expect(result.toasts[0].title).toBe("Test");
  });

  it("ADD_TOAST respects limit of 1", () => {
    const state = { toasts: [{ id: "1", title: "First", open: true } as any] };
    const result = reducer(state, {
      type: "ADD_TOAST",
      toast: { id: "2", title: "Second", open: true } as any,
    });
    expect(result.toasts).toHaveLength(1);
    expect(result.toasts[0].title).toBe("Second");
  });

  it("UPDATE_TOAST updates matching toast", () => {
    const state = { toasts: [{ id: "1", title: "Old", open: true } as any] };
    const result = reducer(state, {
      type: "UPDATE_TOAST",
      toast: { id: "1", title: "New" },
    });
    expect(result.toasts[0].title).toBe("New");
  });

  it("UPDATE_TOAST ignores non-matching toast", () => {
    const state = { toasts: [{ id: "1", title: "Old", open: true } as any] };
    const result = reducer(state, {
      type: "UPDATE_TOAST",
      toast: { id: "999", title: "New" },
    });
    expect(result.toasts[0].title).toBe("Old");
  });

  it("DISMISS_TOAST sets open to false", () => {
    const state = { toasts: [{ id: "1", title: "Test", open: true } as any] };
    const result = reducer(state, { type: "DISMISS_TOAST", toastId: "1" });
    expect(result.toasts[0].open).toBe(false);
  });

  it("DISMISS_TOAST without id dismisses all", () => {
    const state = {
      toasts: [{ id: "1", open: true } as any],
    };
    const result = reducer(state, { type: "DISMISS_TOAST" });
    expect(result.toasts.every((t: any) => t.open === false)).toBe(true);
  });

  it("REMOVE_TOAST removes specific toast", () => {
    const state = { toasts: [{ id: "1" } as any, { id: "2" } as any] };
    const result = reducer(state, { type: "REMOVE_TOAST", toastId: "1" });
    expect(result.toasts).toHaveLength(1);
    expect(result.toasts[0].id).toBe("2");
  });

  it("REMOVE_TOAST without id clears all", () => {
    const state = { toasts: [{ id: "1" } as any, { id: "2" } as any] };
    const result = reducer(state, { type: "REMOVE_TOAST", toastId: undefined });
    expect(result.toasts).toHaveLength(0);
  });
});

describe("toast function", () => {
  it("creates a toast and returns id, dismiss, and update", () => {
    const result = toast({ title: "Hello" });
    expect(result.id).toBeDefined();
    expect(typeof result.dismiss).toBe("function");
    expect(typeof result.update).toBe("function");
  });

  it("returns unique id for each toast", () => {
    const r1 = toast({ title: "First" });
    const r2 = toast({ title: "Second" });
    expect(r1.id).not.toBe(r2.id);
  });

  it("dismiss marks toast as closed when called", () => {
    const { result: hookResult } = renderHook(() => useToast());
    let toastId: string;
    act(() => {
      const t = toast({ title: "To dismiss" });
      toastId = t.id;
    });
    act(() => {
      const found = hookResult.current.toasts.find((x) => x.id === toastId);
      expect(found?.open).toBe(true);
    });
    act(() => {
      hookResult.current.dismiss(toastId);
    });
    const found = hookResult.current.toasts.find((x) => x.id === toastId);
    expect(found?.open).toBe(false);
  });

  it("update modifies toast via returned function", () => {
    const { result: hookResult } = renderHook(() => useToast());
    let updateFn: (props: any) => void;
    act(() => {
      const t = toast({ title: "Original" });
      updateFn = t.update;
    });
    act(() => {
      updateFn!({ title: "Updated" });
    });
    const updated = hookResult.current.toasts.find((x) => x.title === "Updated");
    expect(updated).toBeDefined();
  });
});

describe("useToast hook", () => {
  it("returns toasts, toast, and dismiss from state", () => {
    const { result } = renderHook(() => useToast());
    expect(result.current.toasts).toBeDefined();
    expect(Array.isArray(result.current.toasts)).toBe(true);
    expect(typeof result.current.toast).toBe("function");
    expect(typeof result.current.dismiss).toBe("function");
  });

  it("shows new toast after calling toast()", () => {
    const { result } = renderHook(() => useToast());
    act(() => {
      result.current.toast({ title: "Test toast" });
    });
    expect(result.current.toasts.length).toBeGreaterThan(0);
    expect(result.current.toasts.some((t) => t.title === "Test toast")).toBe(true);
  });

  it("dismiss removes toast from open state", () => {
    const { result } = renderHook(() => useToast());
    let toastId: string;
    act(() => {
      const t = result.current.toast({ title: "To dismiss" });
      toastId = t.id;
    });
    act(() => {
      result.current.dismiss(toastId);
    });
    const found = result.current.toasts.find((t) => t.id === toastId);
    expect(found?.open).toBe(false);
  });
});
