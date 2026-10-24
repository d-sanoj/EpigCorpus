/* A multi-select with type-to-search.
 *
 * Options carry an optional short form (the EDCS Latin token or language
 * code), shown dimmed under the English label and searchable too. Only the
 * first matches are rendered, so the ~22,000 places stay quick to filter.
 */
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
      <span id={`${id}-label`} className="text-sm text-slate-300">
        {label}
      </span>
      <div
        className="field flex min-h-10 cursor-text flex-wrap items-center gap-1.5"
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
            className="inline-flex max-w-full items-center gap-1 rounded-md border border-orange-500/55 bg-orange-500/15 py-0.5 pr-1 pl-2 text-sm"
          >
            <span className="truncate" title={value}>
              {value}
            </span>
            <button
              type="button"
              className="px-1 leading-none text-slate-400 hover:text-white"
              aria-label={`Remove ${value}`}
              onClick={() => remove(value)}
            >
              ×
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
          className="min-w-16 flex-1 bg-transparent py-0.5 outline-none placeholder:text-slate-500"
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
        className="absolute top-full right-0 left-0 z-[1100] mt-1 max-h-72 overflow-y-auto rounded-lg border border-slate-700 bg-slate-900 py-1 shadow-xl shadow-black/60"
        // Keep focus in the input so a click lands before the list closes.
        onMouseDown={(event) => event.preventDefault()}
      >
        {shown.map((value, index) => (
          <li
            key={value}
            id={`${id}-opt-${index}`}
            role="option"
            aria-selected={index === active}
            className="cursor-pointer px-3 py-1.5 leading-tight hover:bg-slate-800 aria-selected:bg-slate-800"
            onClick={() => pick(value)}
          >
            {value}
            {shortForms[value] && (
              <span className="block text-xs text-slate-500">{shortForms[value]}</span>
            )}
          </li>
        ))}
        {!shown.length && (
          <li className="px-3 py-1.5 text-sm text-slate-500">
            {needle.trim() ? "No match" : "Nothing left to choose"}
          </li>
        )}
        {matches.length > shown.length && (
          <li className="px-3 py-1.5 text-sm text-slate-500">
            {formatInteger(matches.length - shown.length)} more — type to narrow
          </li>
        )}
      </ul>
    </div>
  );
}
