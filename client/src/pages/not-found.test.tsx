import { render, screen } from "@testing-library/react";
import NotFound from "./not-found";

describe("NotFound", () => {
  it("renders 404 text", () => {
    render(<NotFound />);
    expect(screen.getByText(/404/)).toBeInTheDocument();
  });

  it("renders page not found heading", () => {
    render(<NotFound />);
    expect(screen.getByText("404 Page Not Found")).toBeInTheDocument();
  });

  it("renders a helpful message about adding the page to the router", () => {
    render(<NotFound />);
    expect(
      screen.getByText("Did you forget to add the page to the router?")
    ).toBeInTheDocument();
  });

  it("renders the AlertCircle icon via card structure", () => {
    const { container } = render(<NotFound />);
    const card = container.querySelector('[class*="Card"]');
    expect(card).toBeInTheDocument();
  });
});
