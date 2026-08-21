# Licence for derived data

**Applies to:** everything under `data/derived/`, including
`abbrev_pairs_v1.tsv.gz`, the splits in `data/derived/v1/splits/`, and the
`|(...)` symbol inventory.

**Does NOT apply to:** the code (`scripts/`, `src/`, `tests/`), which is MIT —
see `LICENSE`. The two are licensed separately and deliberately.

---

## Status: NOT RELEASED

These files are derived from the Epigraphik-Datenbank Clauss-Slaby (EDCS).
**Permission to redistribute has not been sought or granted.** Until it is,
the derived data must not be published, mirrored, or included in any release
artifact.

See `reports/edcs_permission_request.md` for the drafted request and the
fallback plan.

## What may be shared now

The **code**, the **reports**, and the **measured statistics** — counts,
distributions, model results, `results/all_results.json`. These are our own
observations *about* EDCS, not a redistribution of it.

## What may not

Any file containing EDCS text: the pair tables, the splits, the context
windows, and the `|(...)` inventory's surface forms.

## If permission is granted

The intended licence is **CC BY 4.0** with attribution to EDCS as the source
database, subject to whatever terms EDCS specifies. Their terms take
precedence over this intention.

## If permission is refused or unanswered

Release code and reports only, plus a script that rebuilds the derived tables
from a user's own EDCS harvest. Every finding in the paper survives, because
the findings are observations about the database rather than the database
itself.

## Attribution required in every case

Any use of this work must cite EDCS as the source of the underlying
inscriptions. **[VERIFY — EDCS's preferred citation form, to be confirmed in
their reply to the permission request.]**
