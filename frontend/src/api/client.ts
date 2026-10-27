/* A thin typed client over the EpigCorpus HTTP API (/api/v1).
 *
 * Every type comes from schema.d.ts, generated from the backend's
 * openapi.yaml (npm run gen:api), so a contract change that breaks the UI
 * fails the type check.
 */
import type { components, operations } from "./schema";
import { OPENAPI_URL as STATIC_OPENAPI_URL } from "./static/artifacts";
import { staticGet } from "./static/engine";

export type Schemas = components["schemas"];
export type Facets = Schemas["Facets"];
export type InscriptionSummary = Schemas["InscriptionSummary"];
export type InscriptionPage = Schemas["InscriptionPage"];
export type ProvinceLabel = Schemas["ProvinceLabel"];
export type CityPoint = Schemas["CityPoint"];
export type PipelineRun = Schemas["PipelineRun"];
export type PipelineRunRequest = Partial<Schemas["PipelineRunRequest"]>;
export type Problem = Schemas["Problem"];

type GeoJsonOf<K extends keyof operations> = operations[K] extends {
  responses: { 200: { content: { "application/geo+json": infer Body } } };
}
  ? Body
  : never;

export type ResultPlaces = GeoJsonOf<"getResultPlacesGeoJson">;
export type KnownPlaces = GeoJsonOf<"getKnownPlacesGeoJson">;

export const API = "/api/v1";

/** How the app gets its data.
 *
 * "api" (the default): the FastAPI server answers /api/v1 - npm run dev, the
 * Docker image, and the end-to-end tests all run this way.
 * "static": the corpus is precomputed and queried in the browser, for hosting
 * that cannot run anything (GitHub Pages, built with VITE_DATA_MODE=static).
 * Everything that needs a server steps aside there: the pipeline is not
 * offered, the TSV is built locally, and the PNG is a capture of the map.
 */
export const STATIC_MODE = import.meta.env.VITE_DATA_MODE === "static";

/** The API reference: served by the backend, or a file in the static build. */
export const OPENAPI_URL = STATIC_MODE ? STATIC_OPENAPI_URL : `${API}/openapi.yaml`;

export class ApiError extends Error {
  readonly status: number;
  readonly problem: Partial<Problem>;

  constructor(status: number, problem: Partial<Problem>) {
    super(problem.detail || problem.title || `Request failed (${status})`);
    this.status = status;
    this.problem = problem;
  }
}

export async function getJson<T>(path: string, init?: RequestInit): Promise<T> {
  // Without a server, the same paths are answered from the site's own files
  // (see api/static/engine.ts).
  if (STATIC_MODE) return staticGet<T>(path);
  const response = await fetch(API + path, {
    ...init,
    headers: { Accept: "application/json", ...init?.headers },
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new ApiError(response.status, body as Partial<Problem>);
  return body as T;
}

export function postJson<T>(path: string, body?: unknown): Promise<T> {
  return getJson<T>(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}
