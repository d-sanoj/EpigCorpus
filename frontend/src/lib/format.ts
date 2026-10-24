export function formatInteger(value: number): string {
  return Number(value).toLocaleString("en-US");
}

export function plural(count: number, one: string, many: string): string {
  return `${formatInteger(count)} ${count === 1 ? one : many}`;
}

/** "150", "100 – 200", "? – 50": years as EDCS dates them (negative is BC). */
export function dating(from?: number | null, to?: number | null): string {
  const lo = from ?? null;
  const hi = to ?? null;
  if (lo === null && hi === null) return "";
  if (lo === hi) return String(lo);
  return `${lo ?? "?"} – ${hi ?? "?"}`;
}
