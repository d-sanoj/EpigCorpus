"""Pure parsing of EDCS payloads into inscription rows.

No I/O. `now_iso` reads the clock, which is what stamps `retrieved_at`.
See docs/EDCS_API.md for the payload shapes.
"""

import time

from app.config.settings import IMAGE_URL, MONUMENT_URL


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def format_citation(entry, sources):
    """[source_id, volume, number] -> 'CIL-06, *00226'.

    Mirrors the site's own `citationLabel()` (app.js) exactly: source name,
    then '-volume' if a volume is present, then ', number'. Matching EDCS's
    rendering keeps our `publication` column joinable against theirs.

    The '*' prefix EDCS uses to mark falsae is preserved verbatim -- it is the
    only forgery marker the API exposes (see docs/AUDIT.md F1).
    """
    if not isinstance(entry, list) or not entry:
        return ""
    name = sources.get(entry[0], str(entry[0]))
    value = str(name)
    volume = str(entry[1]).strip() if len(entry) > 1 and entry[1] else ""
    detail = str(entry[2]).strip() if len(entry) > 2 and entry[2] else ""
    if volume:
        value += f"-{volume}"
    if detail:
        value += f", {detail}"
    return value.strip()


def image_url(filename):
    clean = str(filename or "").strip()
    if not clean or len(clean) < 2:
        return ""
    return IMAGE_URL.format(prefix=clean[:2], filename=clean)


def format_dating(not_before, not_after):
    """Human-readable dating, matching the site's own `formatDatingRange()`.

    '-' when undated, a bare year when both bounds agree, otherwise
    'from .. to' with '?' for an open bound. Negative years are BC.
    """
    has_before = not_before not in ("", None)
    has_after = not_after not in ("", None)
    if not has_before and not has_after:
        return "-"
    start = str(not_before) if has_before else "?"
    end = str(not_after) if has_after else "?"
    if start == end:
        return start
    return f"{start} .. {end}"


def parse_monument(payload, monument_id, lookup):
    """One monument payload -> a list of rows, one per inscription."""
    obj = payload.get("d", payload) if isinstance(payload, dict) else {}
    if not isinstance(obj, dict):
        return []

    edcs_id = f"EDCS-{int(monument_id):08d}"
    source_url = MONUMENT_URL.format(
        shard=f"{int(monument_id):08d}"[:3], id8=f"{int(monument_id):08d}"
    )
    retrieved_at = now_iso()

    # ── Place / province / coordinates ──
    geo_id = obj.get("g")
    place_info = lookup["places"].get(geo_id, {}) if geo_id is not None else {}
    province = place_info.get("province", "")
    place = place_info.get("ort", "")
    latitude = place_info.get("lat", "")
    longitude = place_info.get("lon", "")

    # ── Material ──
    material_id = obj.get("m")
    material_info = lookup["materials"].get(material_id, {})
    material = material_info.get("token", "")
    material_en = material_info.get("en", "")

    # ── Citations (`belege` as a list, `publication` as a string) ──
    sources = lookup["sources"]
    belege = [format_citation(entry, sources) for entry in (obj.get("q") or [])]
    belege = [b for b in belege if b]
    publication = " | ".join(belege)

    # ── Photos ──
    photos, credits = [], []
    for photo in obj.get("p") or []:
        if isinstance(photo, dict):
            url = image_url(photo.get("b"))
            if url:
                photos.append(url)
            if photo.get("u"):
                credits.append(str(photo["u"]))
    image_urls = " | ".join(photos)
    photo_credits = " | ".join(dict.fromkeys(credits))

    # ── Comments (absent from the old API) ──
    comments = []
    for comment in obj.get("c") or []:
        if isinstance(comment, dict):
            text = comment.get("k") or ""
            note = comment.get("n")
            comments.append(f"{text} ({note})" if note else str(text))
        elif comment:
            comments.append(str(comment))
    comment_text = " | ".join(c for c in comments if c)

    shared = {
        "edcs_id": edcs_id,
        "province": province,
        "place": place,
        "latitude": latitude,
        "longitude": longitude,
        "material": material,
        "material_en": material_en,
        "belege": belege,
        "publication": publication,
        "image_urls": image_urls,
        "photo": image_urls,
        "photo_credits": photo_credits,
        "comment": comment_text,
        "retrieved_at": retrieved_at,
        "source_url": source_url,
    }

    rows = []
    for index, inscription in enumerate(obj.get("i") or []):
        if not isinstance(inscription, dict):
            continue

        text = inscription.get("t") or ""

        dating = inscription.get("d") or []
        not_before = dating[0] if len(dating) > 0 and dating[0] is not None else ""
        not_after = dating[1] if len(dating) > 1 and dating[1] is not None else ""

        language_ids = inscription.get("s") or []
        language_names, language_codes = [], []
        for language_id in language_ids:
            info = lookup["languages"].get(language_id, {})
            if info.get("en"):
                language_names.append(info["en"])
            if info.get("code"):
                language_codes.append(info["code"])

        category_ids = inscription.get("g") or []
        category, category_en = [], []
        for category_id in category_ids:
            info = lookup["categories"].get(category_id, {})
            category.append(info.get("token", str(category_id)))
            category_en.append(info.get("en", str(category_id)))

        rows.append(
            {
                **shared,
                "record_id": f"{edcs_id}-{index}",
                "inscription_index": index,
                "not_before": not_before,
                "not_after": not_after,
                "dating_from": not_before,
                "dating_to": not_after,
                "raw_dating": format_dating(not_before, not_after),
                "inscription_text": text,
                "language": ", ".join(language_names),
                "language_codes": language_codes,
                "category": category,
                "category_en": category_en,
                "status": category_en,
            }
        )

    # Monument with no inscriptions — still record it, as the original did.
    if not rows:
        rows.append(
            {
                **shared,
                "record_id": f"{edcs_id}-0",
                "inscription_index": 0,
                "not_before": "",
                "not_after": "",
                "dating_from": "",
                "dating_to": "",
                "raw_dating": "",
                "inscription_text": "",
                "language": "",
                "language_codes": [],
                "category": [],
                "category_en": [],
                "status": [],
            }
        )

    return rows
