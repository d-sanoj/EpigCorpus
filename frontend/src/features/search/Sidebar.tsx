/* The sidebar: what to search, the filters, Search, and the downloads. */
import { type FormEvent } from "react";
import { API, type Facets } from "../../api/client";
import {
  FILTER_LABELS,
  FILTERS,
  isQuery,
  queryParams,
  SEARCH_MODES,
  type FilterName,
  type Query,
  type SearchIn,
} from "../../api/query";
import { formatInteger } from "../../lib/format";
import { MultiSelect } from "./MultiSelect";

interface Props {
  draft: Query;
  onDraftChange: (query: Query) => void;
  onSubmit: () => void;
  facets: Facets | null;
  busy: boolean;
  /** The query the map and table currently show; the downloads follow it. */
  shown: Query;
  hasResults: boolean;
}

export function Sidebar({ draft, onDraftChange, onSubmit, facets, busy, shown, hasResults }: Props) {
  function submit(event: FormEvent) {
    event.preventDefault();
    onSubmit();
  }

  function setFilter(name: FilterName, values: string[]) {
    onDraftChange({ ...draft, filters: { ...draft.filters, [name]: values } });
  }

  const pngHref = `${API}/exports/map.png?${queryParams(shown)}`;
  const tsvHref = `${API}/exports/search.tsv?${queryParams(shown)}`;

  return (
    <form
      onSubmit={submit}
      autoComplete="off"
      noValidate
      aria-label="Search"
      className="flex flex-col gap-4"
    >
      <div className="flex flex-col gap-1.5">
        <label htmlFor="search-in" className="text-sm text-slate-300">
          Search in
        </label>
        <select
          id="search-in"
          className="field"
          value={draft.searchIn}
          onChange={(event) =>
            onDraftChange({ ...draft, searchIn: event.target.value as SearchIn })
          }
        >
          {Object.entries(SEARCH_MODES).map(([value, mode]) => (
            <option key={value} value={value}>
              {mode.label}
            </option>
          ))}
        </select>
      </div>

      <div className="flex flex-col gap-1.5">
        <label htmlFor="keyword" className="text-sm text-slate-300">
          Keyword
        </label>
        <input
          id="keyword"
          type="search"
          className="field"
          placeholder="Enter search term (optional)"
          maxLength={200}
          value={draft.q}
          onChange={(event) => onDraftChange({ ...draft, q: event.target.value })}
        />
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
              name === "place" ? `Any (${formatInteger(values.length)} - type to search)` : "Any"
            }
          />
        );
      })}

      <button type="submit" className="button-primary" disabled={busy || !facets}>
        {busy ? "Searching…" : "Search"}
      </button>

      <div className="flex flex-col gap-2 border-t border-slate-800 pt-4">
        <span className="text-xs tracking-wide text-slate-500 uppercase">Download</span>
        <a className="button" href={pngHref} download>
          Map as PNG
        </a>
        {isQuery(shown) && hasResults ? (
          <a className="button" href={tsvHref} download>
            Results as TSV
          </a>
        ) : (
          <span className="button" aria-disabled="true" title="Search for something first">
            Results as TSV
          </span>
        )}
      </div>
    </form>
  );
}
