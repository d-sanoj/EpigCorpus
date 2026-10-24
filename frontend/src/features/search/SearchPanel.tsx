/* The search panel: the keyword, where to look for it, and the filters.
 *
 * Nothing runs until Search is pressed (or Enter in the keyword field), so a
 * query can be put together without the map redrawing at every change. */
import { Loader2, Search, SlidersHorizontal } from "lucide-react";
import { type FormEvent } from "react";
import { type Facets } from "../../api/client";
import {
  emptyQuery,
  FILTER_LABELS,
  FILTERS,
  isQuery,
  type FilterName,
  type Query,
  type SearchIn,
} from "../../api/query";
import { Badge, Button, Kbd, Segmented } from "../../components/ui";
import { formatInteger } from "../../lib/format";
import { MultiSelect } from "./MultiSelect";

interface Props {
  draft: Query;
  onDraftChange: (query: Query) => void;
  onSubmit: () => void;
  facets: Facets | null;
  busy: boolean;
}

const SEARCH_IN: { value: SearchIn; label: string; title: string }[] = [
  { value: "raw", label: "Raw", title: "The inscription as EDCS gives it, editorial marks and all" },
  { value: "interpretive", label: "Interpretive", title: "Abbreviations expanded, restorations kept" },
  { value: "conservative", label: "Conservative", title: "Only the letters on the stone" },
];

export function SearchPanel({ draft, onDraftChange, onSubmit, facets, busy }: Props) {
  function submit(event: FormEvent) {
    event.preventDefault();
    onSubmit();
  }

  function setFilter(name: FilterName, values: string[]) {
    onDraftChange({ ...draft, filters: { ...draft.filters, [name]: values } });
  }

  const activeFilters = FILTERS.reduce((sum, name) => sum + draft.filters[name].length, 0);

  return (
    <form
      onSubmit={submit}
      autoComplete="off"
      noValidate
      aria-label="Search"
      className="flex h-full min-h-0 flex-col"
    >
      <div className="flex min-h-0 flex-1 flex-col gap-6 overflow-y-auto px-5 pt-5 pb-6">
        <section className="flex flex-col gap-3">
          <label htmlFor="keyword" className="text-[0.78rem] font-medium text-fg-2">
            Keyword
          </label>
          <div className="field flex h-10 items-center gap-2.5 pr-2">
            <Search className="size-4 flex-none text-muted" aria-hidden="true" />
            <input
              id="keyword"
              type="search"
              className="h-full min-w-0 flex-1 bg-transparent text-sm outline-none placeholder:text-muted [&::-webkit-search-cancel-button]:hidden"
              placeholder="viator, Augustus, coniugi…"
              maxLength={200}
              value={draft.q}
              onChange={(event) => onDraftChange({ ...draft, q: event.target.value })}
            />
            <Kbd>↵</Kbd>
          </div>
          <div className="flex flex-col gap-1.5">
            <span id="search-in-label" className="text-[0.72rem] text-muted">
              Search in
            </span>
            <Segmented
              label="Search in"
              value={draft.searchIn}
              options={SEARCH_IN}
              onChange={(searchIn) => onDraftChange({ ...draft, searchIn })}
              size="sm"
            />
          </div>
        </section>

        <section className="flex flex-col gap-4">
          <div className="flex items-center justify-between">
            <h2 className="flex items-center gap-2 text-[0.78rem] font-semibold text-fg">
              <SlidersHorizontal className="size-3.5 text-muted" aria-hidden="true" />
              Filters
              {activeFilters > 0 && <Badge tone="accent">{activeFilters}</Badge>}
            </h2>
            {activeFilters > 0 && (
              <button
                type="button"
                className="text-[0.75rem] font-medium text-muted transition-colors hover:text-fg"
                onClick={() => onDraftChange({ ...draft, filters: emptyQuery().filters })}
              >
                Clear filters
              </button>
            )}
          </div>
          {FILTERS.map((name) => {
            const values = facets?.values[name] ?? [];
            return (
              <MultiSelect
                key={name}
                label={FILTER_LABELS[name]}
                values={values}
                shortForms={(facets?.short_forms as Record<string, Record<string, string>>)?.[name]}
                selected={draft.filters[name]}
                onChange={(selected) => setFilter(name, selected)}
                placeholder={
                  name === "place"
                    ? `Any of ${formatInteger(values.length)} places`
                    : `Any ${FILTER_LABELS[name].toLowerCase()}`
                }
              />
            );
          })}
        </section>
      </div>

      <div className="flex gap-2 border-t border-line bg-surface px-5 py-4">
        {isQuery(draft) && (
          <Button variant="ghost" size="lg" onClick={() => onDraftChange(emptyQuery())}>
            Reset
          </Button>
        )}
        <Button type="submit" variant="primary" size="lg" className="flex-1" disabled={busy || !facets}>
          {busy ? <Loader2 className="size-4 animate-spin" aria-hidden="true" /> : <Search className="size-4" aria-hidden="true" />}
          {busy ? "Searching…" : "Search"}
        </Button>
      </div>
    </form>
  );
}
