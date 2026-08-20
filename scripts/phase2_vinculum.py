#!/usr/bin/env python3
"""Phase 2: does EDCS preserve numeral/abbreviation overline marking?

The vinculum (overline) had two distinct epigraphic jobs:
  - over a Roman numeral it multiplied by 1000   (X̄ = 10,000)
  - over letters it marked an abbreviation or nomen sacrum

The brief asks whether EDCS's plain-text transcription preserves either. A
negative is a citable observation about the database and must be reported
either way; D-0008 already logged one U+0305 sighting, so a pure negative is
not expected and the question is really "how much, and on what".

Method: full character census of the raw corpus -- no sampling, no
assumption about which code points might occur. Then, for every combining
mark and every relevant non-ASCII code point found, recover what it sits on.

Read-only. Writes reports/ and data/derived/ only.
"""
from __future__ import annotations
import json, re, unicodedata
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RAW = REPO/"data"/"edcs_inscriptions.jsonl"

# Code points the brief names, plus every block that could carry the job.
NAMED = {0x0305: "COMBINING OVERLINE",
         0x0304: "COMBINING MACRON",
         0x0332: "COMBINING LOW LINE"}
# Unicode Supplemental Punctuation U+2E00-U+2E7F, incl. U+2E13 DOTTED OBELOS
SUPP_PUNCT = range(0x2E00, 0x2E80)
# Number Forms U+2150-U+218F: Roman numeral code points, REVERSED ROMAN
# NUMERAL ONE HUNDRED U+2183 (the reversed C = mulieris sign)
NUMBER_FORMS = range(0x2150, 0x2190)
# Ancient Symbols U+10190-U+101CF: ROMAN SEXTANS/UNCIA/DENARIUS SIGN etc.
ANCIENT_SYM = range(0x10190, 0x101D0)

RE_FIELDS = ("inscription_text",)

def main():
    charct = Counter()
    n_rec = 0
    # combining-mark contexts
    comb_ctx = defaultdict(Counter)      # cp -> Counter of base char
    comb_tok = defaultdict(Counter)      # cp -> Counter of whole token
    comb_recs = defaultdict(set)
    # precomposed letters carrying a macron/overline in their decomposition
    precomp = Counter()
    precomp_recs = defaultdict(set)
    block_hits = defaultdict(Counter)
    block_recs = defaultdict(set)
    # does the mark sit on a Roman numeral or on a letter?
    ROMAN = set("IVXLCDM")
    onwhat = defaultdict(Counter)

    for line in RAW.open(encoding="utf-8"):
        r = json.loads(line)
        n_rec += 1
        t = r.get("inscription_text") or ""
        rid = r["record_id"]
        if not t:
            continue
        charct.update(t)
        for i, ch in enumerate(t):
            cp = ord(ch)
            if unicodedata.combining(ch):
                base = t[i-1] if i else ""
                comb_ctx[cp][base] += 1
                comb_recs[cp].add(rid)
                # recover the whole whitespace token around it
                a = i
                while a > 0 and not t[a-1].isspace(): a -= 1
                b = i
                while b < len(t)-1 and not t[b+1].isspace(): b += 1
                tok = t[a:b+1]
                comb_tok[cp][tok] += 1
                # classify what the mark rides on
                stem = re.sub(r"[^A-Za-z]", "", re.sub(r"\([^)]*\)", "", tok))
                if base.upper() in ROMAN and stem and all(c.upper() in ROMAN for c in stem):
                    onwhat[cp]["roman numeral (all-numeral token)"] += 1
                elif base.upper() in ROMAN:
                    onwhat[cp]["numeral letter inside a word token"] += 1
                elif base.isalpha():
                    onwhat[cp]["non-numeral letter"] += 1
                else:
                    onwhat[cp][f"non-letter base {base!r}"] += 1
            else:
                d = unicodedata.decomposition(ch)
                if d and ("0304" in d or "0305" in d or "0332" in d):
                    precomp[ch] += 1
                    precomp_recs[ch].add(rid)
                if cp in SUPP_PUNCT: block_hits["Supplemental Punctuation U+2E00-2E7F"][ch] += 1; block_recs["Supplemental Punctuation U+2E00-2E7F"].add(rid)
                if cp in NUMBER_FORMS: block_hits["Number Forms U+2150-U+218F"][ch] += 1; block_recs["Number Forms U+2150-U+218F"].add(rid)
                if cp in ANCIENT_SYM: block_hits["Ancient Symbols U+10190-U+101CF"][ch] += 1; block_recs["Ancient Symbols U+10190-U+101CF"].add(rid)

    out = {
        "records": n_rec,
        "total_chars": sum(charct.values()),
        "distinct_chars": len(charct),
        "named_codepoints": {
            f"U+{cp:04X} {name}": {
                "occurrences": sum(comb_ctx[cp].values()),
                "records": len(comb_recs[cp]),
                "bases": dict(comb_ctx[cp].most_common(20)),
                "rides_on": dict(onwhat[cp]),
                "example_tokens": dict(comb_tok[cp].most_common(25)),
            } for cp, name in NAMED.items()
        },
        "all_combining_marks": {
            f"U+{cp:04X} {unicodedata.name(chr(cp),'?')}": {
                "occurrences": sum(v.values()), "records": len(comb_recs[cp]),
                "rides_on": dict(onwhat[cp]),
                "top_tokens": dict(comb_tok[cp].most_common(10)),
            } for cp, v in sorted(comb_ctx.items(), key=lambda x: -sum(x[1].values()))
        },
        "precomposed_with_macron_or_overline": {
            f"{ch} U+{ord(ch):04X} {unicodedata.name(ch,'?')}": {
                "occurrences": n, "records": len(precomp_recs[ch])}
            for ch, n in precomp.most_common()
        },
        "unicode_blocks": {
            b: {"occurrences": sum(c.values()), "records": len(block_recs[b]),
                "chars": {f"{ch} U+{ord(ch):04X} {unicodedata.name(ch,'?')}": n
                          for ch, n in c.most_common(40)}}
            for b, c in block_hits.items()
        },
        "ascii_pipe_occurrences": charct.get("|", 0),
    }
    p = REPO/"data"/"derived"/"phase2_vinculum.json"
    p.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"records {n_rec:,}  chars {sum(charct.values()):,}  distinct {len(charct):,}")
    print(f"ASCII '|' occurrences: {charct.get('|',0):,}\n")
    print("NAMED CODE POINTS")
    for cp, name in NAMED.items():
        n = sum(comb_ctx[cp].values())
        print(f"  U+{cp:04X} {name:<22} {n:>8,} occurrences   {len(comb_recs[cp]):>6,} records")
    print("\nALL COMBINING MARKS FOUND")
    for cp, v in sorted(comb_ctx.items(), key=lambda x: -sum(x[1].values())):
        print(f"  U+{cp:04X} {unicodedata.name(chr(cp),'?'):<34} {sum(v.values()):>7,}  recs {len(comb_recs[cp]):>6,}")
    print("\nPRECOMPOSED MACRON/OVERLINE LETTERS")
    if precomp:
        for ch, n in precomp.most_common(20):
            print(f"  {ch} U+{ord(ch):04X} {unicodedata.name(ch,'?'):<38} {n:>7,}")
    else:
        print("  none")
    print("\nUNICODE BLOCKS")
    for b in ["Supplemental Punctuation U+2E00-2E7F","Number Forms U+2150-U+218F","Ancient Symbols U+10190-U+101CF"]:
        c = block_hits.get(b)
        print(f"  {b}: {sum(c.values()) if c else 0:,} occurrences, {len(block_recs.get(b,())):,} records")
        if c:
            for ch, n in c.most_common(12):
                print(f"      {ch} U+{ord(ch):04X} {unicodedata.name(ch,'?')}  {n:,}")
    print("\nwritten", p)

if __name__ == "__main__":
    main()
