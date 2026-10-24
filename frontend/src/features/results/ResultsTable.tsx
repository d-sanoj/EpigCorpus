/* The table pane: the inscriptions a query matched, a page at a time. */
import { useEffect, useState } from "react";
import { getJson, type InscriptionPage, type InscriptionSummary } from "../../api/client";
import { filterParams, queryParams, SEARCH_MODES, type Query } from "../../api/query";
import { dating, formatInteger } from "../../lib/format";

export const TABLE_PAGE = 100;

interface Props {
  query: Query;
  total: number;
}

export function ResultsTable({ query, total }: Props) {
  const [rows, setRows] = useState<InscriptionSummary[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // With a keyword: /inscriptions/search; filters alone: /inscriptions,
  // which takes no q or search_in.
  function page(after: string | null) {
    const extra: Record<string, string | number> = { limit: TABLE_PAGE };
    if (after) extra.cursor = after;
    return query.q
      ? `/inscriptions/search?${queryParams(query, extra)}`
      : `/inscriptions?${filterParams(query, extra)}`;
  }

  useEffect(() => {
    let current = true;
    setRows([]);
    setCursor(null);
    setError(null);
    setLoading(true);
    getJson<InscriptionPage>(page(null))
      .then((body) => {
        if (!current) return;
        setRows(body.data);
        setCursor(body.page.next_cursor ?? null);
      })
      .catch((reason: Error) => current && setError(reason.message))
      .finally(() => current && setLoading(false));
    return () => {
      current = false;
    };
  }, [query]);

  function more() {
    setLoading(true);
    getJson<InscriptionPage>(page(cursor))
      .then((body) => {
        setRows((shown) => [...shown, ...body.data]);
        setCursor(body.page.next_cursor ?? null);
      })
      .catch((reason: Error) => setError(reason.message))
      .finally(() => setLoading(false));
  }

  const mode = SEARCH_MODES[query.searchIn];
  return (
    <section aria-label="Results" className="flex h-full min-h-0 flex-col gap-2">
      <div className="min-h-0 flex-1 overflow-auto rounded-xl border border-slate-800 bg-slate-950/80">
        <table className="w-full border-collapse text-[0.82rem]">
          <thead>
            <tr>
              {["Record", "Province", "Place", "Date", "Language", "Material", "Category"].map(
                (heading) => (
                  <th key={heading} scope="col">
                    {heading}
                  </th>
                ),
              )}
              <th scope="col">{mode.heading}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.record_id} className="hover:bg-slate-900">
                <td className="whitespace-nowrap tabular-nums">{row.record_id}</td>
                <td>{row.province}</td>
                <td>{row.place}</td>
                <td className="whitespace-nowrap">{dating(row.not_before, row.not_after)}</td>
                <td>{row.language}</td>
                <td>{row.material_en}</td>
                <td>{(row.category_en ?? []).join(", ")}</td>
                <td className="min-w-[22rem]">{row[mode.field] ?? ""}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="flex items-center justify-between gap-4 text-sm text-slate-400">
        <span role="status">
          {error
            ? error
            : `Showing ${formatInteger(rows.length)} of ${formatInteger(total)} inscriptions`}
        </span>
        {cursor && (
          <button type="button" className="button w-auto px-5" onClick={more} disabled={loading}>
            Show more
          </button>
        )}
      </div>
    </section>
  );
}
