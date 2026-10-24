/* A query as the UI holds it, and its forms in URLs: the API's parameters
 * and the address bar (the same parameters, so a search can be shared). */

export const FILTERS = ["language", "province", "place", "category", "material"] as const;
export type FilterName = (typeof FILTERS)[number];

export const SEARCH_MODES = {
  raw: { label: "Raw inscriptions", field: "inscription_text", heading: "Raw inscription" },
  interpretive: {
    label: "Interpretive Cleaned Inscriptions",
    field: "inscription_text_interpretive",
    heading: "Interpretive text",
  },
  conservative: {
    label: "Conservative Cleaned Inscriptions",
    field: "inscription_text_conservative",
    heading: "Conservative text",
  },
} as const;
export type SearchIn = keyof typeof SEARCH_MODES;

export const FILTER_LABELS: Record<FilterName, string> = {
  language: "Language",
  province: "Province",
  place: "Place",
  category: "Category",
  material: "Material",
};

export interface Query {
  q: string;
  searchIn: SearchIn;
  filters: Record<FilterName, string[]>;
}

export function emptyFilters(): Record<FilterName, string[]> {
  return { language: [], province: [], place: [], category: [], material: [] };
}

export function emptyQuery(): Query {
  return { q: "", searchIn: "raw", filters: emptyFilters() };
}

/** A keyword, any filter, or both: anything else shows every known place. */
export function isQuery(query: Query): boolean {
  return Boolean(query.q) || FILTERS.some((name) => query.filters[name].length > 0);
}

function appendFilters(params: URLSearchParams, query: Query) {
  for (const name of FILTERS) {
    for (const value of query.filters[name]) params.append(name, value);
  }
}

/** Parameters for the query operations: q, search_in and the filters. */
export function queryParams(query: Query, extra: Record<string, string | number> = {}): string {
  const params = new URLSearchParams();
  if (query.q) params.append("q", query.q);
  params.append("search_in", query.searchIn);
  appendFilters(params, query);
  for (const [key, value] of Object.entries(extra)) params.append(key, String(value));
  return params.toString();
}

/** Parameters for GET /inscriptions, which takes only the filters. */
export function filterParams(query: Query, extra: Record<string, string | number> = {}): string {
  const params = new URLSearchParams();
  appendFilters(params, query);
  for (const [key, value] of Object.entries(extra)) params.append(key, String(value));
  return params.toString();
}

export function fromSearch(search: string): Query {
  const params = new URLSearchParams(search);
  const filters = emptyFilters();
  for (const name of FILTERS) filters[name] = params.getAll(name);
  const searchIn = params.get("search_in");
  return {
    q: (params.get("q") ?? "").trim(),
    searchIn: searchIn && searchIn in SEARCH_MODES ? (searchIn as SearchIn) : "raw",
    filters,
  };
}

export function toSearch(query: Query): string {
  return isQuery(query) ? `?${queryParams(query)}` : "";
}
