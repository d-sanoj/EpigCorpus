# ADR 0002 — Design the HTTP API contract before implementing it

- **Status:** Accepted, implemented (see the Implementation note at the end)
- **Date:** 2026-10-23
- **Relates to:** ADR 0001 (layered `app/` layout), ADR 0003 (authentication, proposed), ADR 0004 (Streamlit replaced)
- **Artifacts:** `app/api/openapi.yaml`, `app/api/README.md`

## Context

After ADR 0001 the search, cleaning, map-data and scrape logic lives in
`app/services`, callable without Streamlit. The Streamlit map is today the
only way to use most of it, and the scraper is only a command line. Several
uses need the same capabilities over HTTP: scripts and notebooks that query
the corpus without loading 588k rows, other projects that want the cleaned
variants, a future UI, and starting a harvest without shell access.

The project has been burned by interfaces that changed under it. EDCS's
2026-08-07 release removed `/api/query` and broke both LatEpig and EpigCorpus
(`docs/LATEPIG_BREAKAGE.md`), and nothing in either project noticed until
data stopped arriving. `app/tests/adapters/test_edcs_upstream_contract.py`
exists because of that.

## Decision

1. **The contract comes first.** `app/api/openapi.yaml` (OpenAPI 3.1) is
   written, reviewed and agreed before any endpoint is implemented. It is
   versioned under `/v1`, paginates with opaque cursors, reports errors as
   RFC 9457 `application/problem+json`, runs scrape jobs asynchronously
   (`202` + `Location`, then poll), and documents its filters to match the
   existing search exactly.
2. **Every operation maps onto an existing service.** Resources were taken
   from what `app/services` already does, not invented. `app/api/README.md`
   names the service call behind each operation and lists the gaps
   (cancellation and progress hooks for scrape jobs, the outcome return, the
   bounding-box question, timestamp format) that implementation must close.
3. **The implementation will be a thin adapter.** `app/api` may import only
   config, models and services (`app/tests/test_layering.py`). Its job is
   routing, validation, pagination, serialisation and problem responses, not
   logic.
4. **The UI will later become a client of this API**, not of the services
   directly. The current Streamlit app stays frozen (ADR 0001) and keeps
   calling the services in-process until a replacement UI is built against
   `/v1`.

## Why contract first

- **Review before cost.** Resource names, filter semantics, pagination and
  error shapes are the parts that are expensive to change once clients exist.
  A YAML file is cheap to argue over; a deployed endpoint is not.
- **The contract is the test oracle.** Once endpoints exist, contract tests
  can check responses against `openapi.yaml`. That is the same discipline the
  upstream contract tests apply to EDCS, turned on ourselves, so our own
  clients learn about a breaking change from a red test.
- **It exposed real questions early.** Writing the contract against the
  services surfaced behaviour nobody had decided on: search silently drops the
  19,938 rows without coordinates, the TSV export is per monument rather than
  per inscription, `retrieved_at` is stored as epoch milliseconds in the
  cleaned corpus, and a scrape cannot be cancelled except by a signal. Each is
  now a recorded question instead of an accident of implementation.
- **Framework choice stays open.** Nothing in the contract depends on a web
  framework, and no dependency has been added. The framework can be chosen
  later by how well it serves this contract.

## Why the UI will be a client of the API

- **One behaviour, one place.** Today the map's search semantics are shared
  with the API only by convention (both call `services.search`). Once the UI
  calls `/v1`, search, filters, pagination and exports behave identically for
  the UI and every other client by construction.
- **The API gets a demanding first client.** A UI exercises every read
  operation, so gaps in the contract show up during development rather than
  in a researcher's notebook.
- **Separate deployment and scaling.** The Streamlit process currently holds
  the whole corpus in memory per server. An API process can hold it once and
  serve a light UI.
- **Auth in one place.** When ADR 0003 settles the flows, the UI signs in like
  any other client. There is no second, UI-only permission model.

## Consequences

- Until endpoints are implemented, the API delivers nothing to users. The
  contract is a plan, and the README says so.
- The contract must be kept in step with the services. Adding a filter to the
  map without adding it to `openapi.yaml` is the drift this ADR exists to
  prevent. Review checks both.
- Implementing scrape jobs requires small, deliberate service changes (stop
  hook, progress port, a returned outcome). Each will be its own reviewed
  change with its own characterisation tests.
- Rebuilding the UI on the API is future work and is out of scope for the
  restructure. The frozen Streamlit app remains the supported UI until then.

## Alternatives considered

- **Code first, generate the spec from the framework.** This is faster to a
  first endpoint, but it makes the spec a by-product, couples the contract to
  a framework choice we have not made, and leaves the open questions above to
  be settled by whatever the first implementation happens to do.
- **Expose the services through the Streamlit app.** Streamlit is not an
  HTTP API server, and this would tie every client to the UI process.

## Implementation note (2026-10-23)

The maintainer asked for the API to be implemented and for Streamlit to be
replaced by a UI with the same features. Both are done:

- `app/api` implements every operation in the contract, and
  `app/tests/api/test_contract.py` checks each response against it.
- The open questions above were settled conservatively, keeping today's
  behaviour, and the choices are recorded in `app/api/README.md`.
- Four additions the UI needed went into the contract first: health, the PNG
  export, place-page summaries, and a larger page size for places.
- The UI became a client of `/v1` straight away rather than later; see
  ADR 0004.

