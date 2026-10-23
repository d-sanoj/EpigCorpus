# ADR 0003 — API authentication flows

- **Status:** Proposed. **Open: no decision taken.**
- **Date:** 2026-10-23
- **Relates to:** ADR 0002 (API-first design), `app/api/openapi.yaml`

## Context

`app/api/openapi.yaml` states that the API will be protected with OAuth 2.0 /
OpenID Connect and declares an `openIdConnect` security scheme with a
placeholder discovery URL. No operation requires it yet. This ADR holds the
decision on *which* flows, scopes and identity provider, which was
deliberately left open.

What the API exposes, by sensitivity:

| Operations | Risk |
|---|---|
| Reads: inscriptions, variants, places, map layers, lookups, facets | Public research data, but redistribution terms for the EDCS-derived corpus are unsettled (`app/data/README.md`, T29) |
| Exports: whole-corpus and search downloads | Same terms question, and large responses |
| `POST /v1/scrape-jobs`, `…/cancel` | Load on a third-party server (EDCS). `restart: true` deletes the corpus output |

## Questions to decide

1. **Identity provider.** Institutional (university SSO) or hosted (Auth0,
   Keycloak, GitHub as OIDC), or none for reads.
2. **Flows.**
   - Browser UI (ADR 0002): authorization code + PKCE is the usual choice.
   - Automation and notebooks: client credentials, or personal access tokens
     issued by the provider.
   - Device code for command-line clients?
3. **Scopes.** A starting proposal to argue with, not a decision:
   `corpus:read` (reads, facets, lookups), `corpus:export` (downloads),
   `harvest:run` (start and cancel scrape jobs), with `restart: true` possibly
   behind a separate `harvest:restart`.
4. **Anonymous reads.** Should read operations be public, rate-limited per IP,
   and exports and harvests require a token?
5. **Where tokens are checked.** In the API layer only (services stay
   auth-agnostic), or also at a gateway.

## Decision

None yet. When decided, record the provider, flows, scopes and the
anonymous-access rule here, then update `components.securitySchemes` and the
per-operation `security` in `app/api/openapi.yaml` in the same change.
