/* The results drawer: a bar along the bottom of the map, opening into the
 * table of matching inscriptions.
 *
 * Collapsed, it says what matched and offers the TSV download, and the map
 * keeps the whole pane. Opened, it shows the inscriptions a page at a time;
 * rows are fetched only once it is opened. */
import { ChevronUp, Download, Loader2, Table2 } from "lucide-react";
import { useEffect, useState } from "react";
import {
  API,
  getJson,
  STATIC_MODE,
  type InscriptionPage,
  type InscriptionSummary,
} from "../../api/client";
import { filterParams, queryParams, SEARCH_MODES, type Query } from "../../api/query";
import { staticTsv } from "../../api/static/engine";
import { Button, cx, LinkButton } from "../../components/ui";
import { dating, formatInteger, plural } from "../../lib/format";

export const TABLE_PAGE = 100;

interface Props {
  query: Query;
  inscriptions: number;
  places: number;
}

// With a keyword: /inscriptions/search; filters alone: /inscriptions, which
// takes no q or search_in.
function pagePath(query: Query, after: string | null): string {
  const extra: Record<string, string | number> = { limit: TABLE_PAGE };
  if (after) extra.cursor = after;
  return query.q
    ? `/inscriptions/search?${queryParams(query, extra)}`
    : `/inscriptions?${filterParams(query, extra)}`;
}

export function ResultsDrawer({ query, inscriptions, places }: Props) {
  const [open, setOpen] = useState(false);
  const [rows, setRows] = useState<InscriptionSummary[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadedFor, setLoadedFor] = useState<Query | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [preparing, setPreparing] = useState(false);

  // The first page loads when the drawer opens, and again for each new query
  // while it is open.
  useEffect(() => {
    if (!open || loadedFor === query) return;
    let current = true;
    setRows([]);
    setCursor(null);
    setError(null);
    setLoading(true);
    getJson<InscriptionPage>(pagePath(query, null))
      .then((body) => {
        if (!current) return;
        setRows(body.data);
        setCursor(body.page.next_cursor ?? null);
        setLoadedFor(query);
      })
      .catch((reason: Error) => current && setError(reason.message))
      .finally(() => current && setLoading(false));
    return () => {
      current = false;
    };
  }, [open, query, loadedFor]);

  function more() {
    setLoading(true);
    getJson<InscriptionPage>(pagePath(query, cursor))
      .then((body) => {
        setRows((shown) => [...shown, ...body.data]);
        setCursor(body.page.next_cursor ?? null);
      })
      .catch((reason: Error) => setError(reason.message))
      .finally(() => setLoading(false));
  }

  const mode = SEARCH_MODES[query.searchIn];
  const tsvHref = `${API}/exports/search.tsv?${queryParams(query)}`;

  // With no server to stream the export, the page builds it from the rows it
  // holds and hands it over as a download.
  async function downloadTsv() {
    setPreparing(true);
    setError(null);
    try {
      const { blob, filename } = await staticTsv(queryParams(query));
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = filename;
      link.click();
      URL.revokeObjectURL(url);
    } catch (reason) {
      setError((reason as Error).message);
    } finally {
      setPreparing(false);
    }
  }

  return (
    <section
      aria-label="Results"
      className={cx(
        "float-card absolute right-3 bottom-3 left-3 z-[1001] flex flex-col overflow-hidden",
        "transition-[height] duration-300 ease-out sm:right-16 sm:left-36",
        open ? "h-[min(55%,580px)]" : "h-12",
      )}
    >
      <header className="flex h-12 flex-none items-center gap-3 pr-2 pl-4">
        <Table2 className="size-4 flex-none text-muted" aria-hidden="true" />
        <div className="min-w-0 flex-1 truncate text-[0.82rem]">
          <span className="font-semibold text-fg tabular-nums">
            {plural(inscriptions, "inscription", "inscriptions")}
          </span>
          <span className="text-muted"> at {plural(places, "place", "places")}</span>
        </div>
        {STATIC_MODE ? (
          <Button
            size="sm"
            variant="ghost"
            onClick={downloadTsv}
            disabled={preparing}
            title="The matching inscriptions, with the columns this site holds, tab-separated"
          >
            {preparing ? (
              <Loader2 className="size-3.5 animate-spin" aria-hidden="true" />
            ) : (
              <Download className="size-3.5" aria-hidden="true" />
            )}
            <span className="hidden sm:inline">{preparing ? "Preparing" : "Download"}</span> TSV
          </Button>
        ) : (
          <LinkButton
            href={tsvHref}
            download
            size="sm"
            variant="ghost"
            title="Every column of the matching inscriptions, tab-separated"
          >
            <Download className="size-3.5" aria-hidden="true" />
            <span className="hidden sm:inline">Download</span> TSV
          </LinkButton>
        )}
        <Button
          size="sm"
          variant="secondary"
          aria-expanded={open}
          onClick={() => setOpen((value) => !value)}
        >
          {open ? "Hide table" : "Show table"}
          <ChevronUp
            className={cx("size-3.5 transition-transform duration-300", open && "rotate-180")}
            aria-hidden="true"
          />
        </Button>
      </header>

      {open && (
        <>
          <div className="min-h-0 flex-1 overflow-auto border-t border-line">
            <table className="data-table w-full border-collapse">
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
                  <tr key={row.record_id}>
                    <td className="font-medium whitespace-nowrap text-fg tabular-nums">
                      {row.record_id}
                    </td>
                    <td>{row.province}</td>
                    <td>{row.place}</td>
                    <td className="whitespace-nowrap tabular-nums">
                      {dating(row.not_before, row.not_after)}
                    </td>
                    <td>{row.language}</td>
                    <td>{row.material_en}</td>
                    <td>{(row.category_en ?? []).join(", ")}</td>
                    <td className="min-w-[24rem] text-fg">{row[mode.field] ?? ""}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {loading && rows.length === 0 && (
              <div className="flex items-center justify-center gap-2 py-10 text-sm text-muted">
                <Loader2 className="size-4 animate-spin" aria-hidden="true" /> Loading inscriptions…
              </div>
            )}
          </div>
          <footer className="flex h-11 flex-none items-center justify-between gap-4 border-t border-line px-4 text-[0.78rem] text-muted">
            <span role="status">
              {error
                ? error
                : `Showing ${formatInteger(rows.length)} of ${formatInteger(inscriptions)} inscriptions`}
            </span>
            {cursor && (
              <Button size="sm" variant="ghost" onClick={more} disabled={loading}>
                {loading && <Loader2 className="size-3.5 animate-spin" aria-hidden="true" />}
                Show more
              </Button>
            )}
          </footer>
        </>
      )}
    </section>
  );
}
