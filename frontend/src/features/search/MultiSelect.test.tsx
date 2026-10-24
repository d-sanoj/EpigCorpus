import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { MultiSelect, RENDER_LIMIT } from "./MultiSelect";

function Harness({ values, onChange }: { values: string[]; onChange?: (v: string[]) => void }) {
  const [selected, setSelected] = useState<string[]>([]);
  return (
    <MultiSelect
      label="Language"
      values={values}
      shortForms={{ Latin: "la", Greek: "gr" }}
      selected={selected}
      onChange={(next) => {
        setSelected(next);
        onChange?.(next);
      }}
    />
  );
}

describe("MultiSelect", () => {
  it("filters by label and by short form, and picks with Enter", async () => {
    const user = userEvent.setup();
    const changes: string[][] = [];
    render(<Harness values={["Greek", "Latin", "Etruscan"]} onChange={(v) => changes.push(v)} />);
    const box = screen.getByRole("combobox", { name: "Language" });
    await user.type(box, "gr");
    expect(screen.getAllByRole("option").map((o) => o.firstChild?.textContent)).toEqual(["Greek"]);
    await user.keyboard("{Enter}");
    expect(changes.at(-1)).toEqual(["Greek"]);
    expect(screen.getByRole("button", { name: "Remove Greek" })).toBeInTheDocument();
  });

  it("removes the last choice with Backspace", async () => {
    const user = userEvent.setup();
    const changes: string[][] = [];
    render(<Harness values={["Greek", "Latin"]} onChange={(v) => changes.push(v)} />);
    const box = screen.getByRole("combobox", { name: "Language" });
    await user.click(box);
    await user.click(screen.getByRole("option", { name: /^Latin/ }));
    await user.keyboard("{Backspace}");
    expect(changes).toEqual([["Latin"], []]);
  });

  it("renders only the first matches of a long list", async () => {
    const user = userEvent.setup();
    const values = Array.from({ length: RENDER_LIMIT + 50 }, (_, i) => `Place ${i}`);
    render(<Harness values={values} />);
    await user.click(screen.getByRole("combobox", { name: "Language" }));
    expect(screen.getAllByRole("option")).toHaveLength(RENDER_LIMIT);
    expect(screen.getByText("50 more — type to narrow")).toBeInTheDocument();
  });
});
