"""Domain shapes: the column schema of an inscription record.

One row per inscription; record_id = edcs_id + inscription index, e.g.
EDCS-00000001-0. These names are the contract between the scraper's output,
the cleaner, the search services and the exports.
"""

# ─── OUTPUT COLUMNS ───────────────────────────────────────────────────────────
# The first block is the original EpigCorpus schema, unchanged, so main.py,
# the cleaner, the search services and the API read the same names.
# The second block adds the remaining descriptive fields plus record-level provenance.
TSV_FIELDS = [
    # — original EpigCorpus columns —
    "record_id",
    "edcs_id",
    "inscription_index",
    "province",
    "place",
    "latitude",
    "longitude",
    "material",
    "material_en",
    "not_before",
    "not_after",
    "inscription_text",
    "language",
    "category",
    "category_en",
    "belege",
    "image_urls",
    # — additional descriptive fields —
    "publication",      # citations as a single display string
    "raw_dating",       # human-readable dating range
    "dating_from",      # earliest year (negative = BC)
    "dating_to",        # latest year (negative = BC)
    "status",           # inscription genus / personal status
    "comment",          # editorial comments on the monument
    "photo",            # resolved image URLs
    "language_codes",
    "photo_credits",
    # — provenance —
    "retrieved_at",
    "source_url",
]


LIST_COLUMNS = {"category", "category_en", "belege", "status", "language_codes"}


# ─── Search ───────────────────────────────────────────────────────────────────
# Required columns - everything else is dropped to reduce memory load
REQUIRED_COLS = [
    "latitude",
    "longitude",
    "inscription_text",
    "inscription_text_interpretive",
    "inscription_text_conservative",
    "record_id",
    "edcs_id",
    "place",
    "province",
    "not_before",
    "not_after",
    "language",
    "material_en",
    "category_en",
]


# Fields whose cells hold several values at once: category_en is a list, and
# language arrives comma-joined ("Greek, Latin"). Selecting "Latin" has to match
# those rows too, so both are filtered on token overlap rather than equality.
MULTI_VALUE_FILTERS = ("category_en", "language")
