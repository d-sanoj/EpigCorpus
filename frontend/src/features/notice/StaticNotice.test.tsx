import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { loadManifest, type Manifest } from "../../api/static/artifacts";
import { StaticNotice } from "./StaticNotice";

// The component reads the corpus's own scrape date from the manifest.
vi.mock("../../api/static/artifacts", () => ({ loadManifest: vi.fn() }));

function manifestWith(dataAsOf: string | null): Manifest {
  return { data_as_of: dataAsOf } as Manifest;
}

describe("StaticNotice", () => {
  beforeEach(() => {
    vi.mocked(loadManifest).mockResolvedValue(manifestWith("2026-08-19"));
  });

  it("dates the snapshot and holds the commands to run it locally", async () => {
    const user = userEvent.setup();
    render(<StaticNotice />);

    expect(screen.getByText("Static snapshot.")).toBeInTheDocument();
    // The manifest loads after the first paint; the date follows it.
    expect(await screen.findByText("19 August 2026")).toBeInTheDocument();

    const local = screen.getByRole("button", { name: /Run it locally/ });
    expect(local).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByText(/docker compose up --build/)).toBeNull();

    await user.click(local);
    expect(local).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByText(/git clone https:\/\/github.com\/d-sanoj\/EpigCorpus.git/)).toBeInTheDocument();
    expect(screen.getByText(/docker compose up --build/)).toBeInTheDocument();
  });

  it("cannot be closed: the disclosure stays on the page", async () => {
    render(<StaticNotice />);
    await screen.findByText("19 August 2026");

    const labels = screen.getAllByRole("button").map((button) => button.textContent?.trim());
    expect(labels).toEqual(["Run it locally"]);
    expect(screen.queryByRole("button", { name: /dismiss|close/i })).toBeNull();
  });

  it("says the corpus is the committed one when it has no date", async () => {
    vi.mocked(loadManifest).mockResolvedValue(manifestWith(null));
    render(<StaticNotice />);

    expect(
      await screen.findByText(/the cleaned corpus committed in the repository/i),
    ).toBeInTheDocument();
    expect(screen.queryByText(/scraped through/)).toBeNull();
  });
});
