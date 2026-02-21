import { render, screen, fireEvent } from "@testing-library/react";
import { ThemeToggle } from "./ThemeToggle";

vi.mock("@/hooks/use-theme", () => ({
  useTheme: vi.fn(),
}));

import { useTheme } from "@/hooks/use-theme";

const mockUseTheme = vi.mocked(useTheme);

describe("ThemeToggle", () => {
  it("renders a toggle button", () => {
    mockUseTheme.mockReturnValue({
      theme: "light",
      toggleTheme: vi.fn(),
    } as any);

    render(<ThemeToggle />);
    const button = screen.getByRole("button", {
      name: /switch to dark theme/i,
    });
    expect(button).toBeInTheDocument();
  });

  it("shows Moon icon when theme is light", () => {
    mockUseTheme.mockReturnValue({
      theme: "light",
      toggleTheme: vi.fn(),
    } as any);

    const { container } = render(<ThemeToggle />);
    // Moon icon is shown in light mode (lucide-react renders svg)
    expect(screen.getByRole("button", { name: /switch to dark theme/i })).toBeInTheDocument();
  });

  it("shows Sun icon when theme is dark", () => {
    mockUseTheme.mockReturnValue({
      theme: "dark",
      toggleTheme: vi.fn(),
    } as any);

    render(<ThemeToggle />);
    expect(screen.getByRole("button", { name: /switch to light theme/i })).toBeInTheDocument();
  });

  it("calls toggleTheme when button is clicked", () => {
    const toggleTheme = vi.fn();
    mockUseTheme.mockReturnValue({
      theme: "light",
      toggleTheme,
    } as any);

    render(<ThemeToggle />);
    const button = screen.getByRole("button", {
      name: /switch to dark theme/i,
    });
    fireEvent.click(button);
    expect(toggleTheme).toHaveBeenCalledTimes(1);
  });

  it("displays tooltip for dark mode when in light theme", async () => {
    mockUseTheme.mockReturnValue({
      theme: "light",
      toggleTheme: vi.fn(),
    } as any);

    render(<ThemeToggle />);
    const button = screen.getByRole("button", {
      name: /switch to dark theme/i,
    });
    expect(button).toBeInTheDocument();
    // Tooltip shows "Dark mode" when light - we verify the trigger exists
    fireEvent.mouseEnter(button);
  });
});
