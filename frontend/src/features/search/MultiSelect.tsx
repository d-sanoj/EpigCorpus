/* A multi-select with type-to-search.
 *
 * Options carry an optional short form (the EDCS Latin token or language
 * code), shown dimmed under the English label and searchable too. Only the
 * first matches are rendered, so the ~22,000 places stay quick to filter.
 */
import { X } from "lucide-react";
import { useId, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { formatInteger } from "../../lib/format";

export const RENDER_LIMIT = 200;

interface Props {
  label: string;
  values: string[];
  shortForms?: Record<string, string>;
  selected: string[];
  onChange: (selected: string[]) => void;
  placeholder?: string;
}

export function MultiSelect({
  label,
  values,
  shortForms = {},
  selected,
  onChange,
  placeholder = "Any",
}: Props) {
  const id = useId();
  const [open, setOpen] = useState(false);
  const [needle, setNeedle] = useState("");
  const [active, setActive] = useState(0);
  const input = useRef<HTMLInputElement>(null);
  const list = useRef<HTMLUListElement>(null);

  const matches = useMemo(() => {
    const term = needle.trim().toLowerCase();
    return values.filter((value) => {
      if (selected.includes(value)) return false;
      if (!term) return true;
      const short = shortForms[value] ?? "";
      return value.toLowerCase().includes(term) || short.toLowerCase().includes(term);
    });
  }, [values, selected, shortForms, needle]);
  const shown = matches.slice(0, RENDER_LIMIT);

  function pick(value: string | undefined) {
    if (value === undefined || selected.includes(value)) return;
    onChange([...selected, value]);
    setNeedle("");
  }

  function remove(value: string) {
    onChange(selected.filter((item) => item !== value));
  }

  function move(to: number) {
    setActive(to);
    requestAnimationFrame(() => {
      list.current?.querySelector('[aria-selected="true"]')?.scrollIntoView({ block: "nearest" });
    });
  }

  function onKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setOpen(true);
      move(Math.min(shown.length - 1, active + 1));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      move(Math.max(0, active - 1));
    } else if (event.key === "Enter") {
      // Enter picks an option while the list is open with text typed;
      // otherwise it falls through and submits the search form.
      if (open && needle.trim() && shown.length) {
        event.preventDefault();
        pick(shown[Math.max(0, active)]);
      }
    } else if (event.key === "Escape") {
      setOpen(false);
      setNeedle("");
    } else if (event.key === "Backspace" && !needle && selected.length) {
      remove(selected[selected.length - 1]);
    }
  }

  const listId = `${id}-list`;
  return (
    <div className="relative flex min-w-0 flex-col gap-1.5">
      <span id={`${id}-label`} className="text-[0.78rem] font-medium text-fg-2">
        {label}
      </span>
      <div
        className="field flex min-h-9 cursor-text flex-wrap items-center gap-1 py-1"
        onMouseDown={(event) => {
          if (event.target === event.currentTarget) {
            event.preventDefault();
            input.current?.focus();
          }
        }}
      >
        {selected.map((value) => (
          <span
            key={value}
            className="inline-flex h-6 max-w-full items-center gap-0.5 rounded-md bg-surface-3 pr-0.5 pl-2 text-[0.78rem] font-medium text-fg"
          >
            <span className="truncate" title={value}>
              {value}
            </span>
            <button
              type="button"
              className="flex size-5 items-center justify-center rounded text-muted transition-colors hover:bg-line hover:text-fg"
              aria-label={`Remove ${value}`}
              onClick={() => remove(value)}
            >
              <X className="size-3" />
            </button>
          </span>
        ))}
        <input
          ref={input}
          type="text"
          role="combobox"
          aria-expanded={open}
          aria-controls={listId}
          aria-labelledby={`${id}-label`}
          aria-autocomplete="list"
          aria-activedescendant={open && shown.length ? `${id}-opt-${active}` : undefined}
          className="h-6 min-w-16 flex-1 bg-transparent px-0.5 outline-none placeholder:text-muted"
          placeholder={selected.length ? "" : placeholder}
          value={needle}
          onFocus={() => {
            setOpen(true);
            setActive(0);
          }}
          onBlur={() => {
            setOpen(false);
            setNeedle("");
          }}
          onChange={(event) => {
            setNeedle(event.target.value);
            setActive(0);
            setOpen(true);
          }}
          onKeyDown={onKeyDown}
        />
      </div>
      <ul
        ref={list}
        id={listId}
        role="listbox"
        aria-multiselectable="true"
        hidden={!open}
        className="float-card absolute top-full right-0 left-0 z-[1100] mt-1.5 max-h-72 overflow-y-auto bg-surface p-1"
        // Keep focus in the input so a click lands before the list closes.
        onMouseDown={(event) => event.preventDefault()}
      >
        {shown.map((value, index) => (
          <li
            key={value}
            id={`${id}-opt-${index}`}
            role="option"
            aria-selected={index === active}
            className="cursor-pointer rounded-md px-2.5 py-1.5 text-[0.82rem] leading-tight text-fg hover:bg-surface-2 aria-selected:bg-surface-2"
            onClick={() => pick(value)}
          >
            {value}
            {shortForms[value] && (
              <span className="mt-0.5 block text-[0.7rem] text-muted">{shortForms[value]}</span>
            )}
          </li>
        ))}
        {!shown.length && (
          <li className="px-2.5 py-2 text-[0.8rem] text-muted">
            {needle.trim() ? "No match" : "Nothing left to choose"}
          </li>
        )}
        {matches.length > shown.length && (
          <li className="px-2.5 py-2 text-[0.75rem] text-muted">
            {formatInteger(matches.length - shown.length)} more — type to narrow
          </li>
        )}
      </ul>
    </div>
  );
}
