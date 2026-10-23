#!/usr/bin/env python3
"""Phase 2b: the three overline sightings in full, and what EDCS uses in
place of the vinculum -- cross-referenced against the Phase 1 |(...) inventory."""
from __future__ import annotations
import json, re, unicodedata
from collections import Counter, defaultdict
from pathlib import Path
REPO = Path(__file__).resolve().parent.parent

MARKS = {"̅", "̄", "̲"}
# the milliary / thousands family from the Phase 1 pipe inventory
THOUS = re.compile(r"\|*\((mil[il]?[ae]?ri?[a-z]*|mille[a-z]*|mili[ae][a-z]*|milli[a-z]*)\)", re.I)
ROMAN_TOK = re.compile(r"^[IVXLCDM]+$")

sightings = []
thous_ctx = Counter()
thous_forms = Counter()
bare_paren_thous = Counter()
adjacent_numeral = Counter()
numeral_values = Counter()
dot_below_tokens = Counter()

def roman_val(s):
    v = {"I":1,"V":5,"X":10,"L":50,"C":100,"D":500,"M":1000}
    tot = 0; prev = 0
    for c in reversed(s.upper()):
        x = v.get(c, 0)
        tot += -x if x < prev else x
        prev = max(prev, x)
    return tot

for line in (REPO/"data"/"edcs_inscriptions.jsonl").open(encoding="utf-8"):
    r = json.loads(line)
    t = r.get("inscription_text") or ""
    if not t: continue
    if any(m in t for m in MARKS):
        for i, ch in enumerate(t):
            if ch in MARKS:
                a = max(0, i-70); b = min(len(t), i+70)
                sightings.append({
                    "record_id": r["record_id"],
                    "mark": f"U+{ord(ch):04X} {unicodedata.name(ch)}",
                    "base": t[i-1] if i else "",
                    "context": t[a:b],
                    "province": r.get("province"),
                })
    if "̣" in t:
        for i, ch in enumerate(t):
            if ch == "̣":
                a = i
                while a > 0 and not t[a-1].isspace(): a -= 1
                b = i
                while b < len(t)-1 and not t[b+1].isspace(): b += 1
                dot_below_tokens[t[a:b+1]] += 1
    # thousands-family device
    for m in THOUS.finditer(t):
        form = m.group(0)
        thous_forms[form] += 1
        a = max(0, m.start()-60); b = min(len(t), m.end()+60)
        if len(thous_ctx) < 4000:
            thous_ctx[t[a:b]] += 1
        if not form.startswith("|"):
            bare_paren_thous[form] += 1
        # the token immediately before and after
        pre = t[:m.start()].split()
        post = t[m.end():].split()
        for tok in ([pre[-1]] if pre else []) + ([post[0]] if post else []):
            tok = tok.strip(",.;:")
            if ROMAN_TOK.match(tok):
                adjacent_numeral[tok] += 1
                numeral_values[roman_val(tok)] += 1

out = {
    "overline_sightings": sightings,
    "thousands_family_forms": dict(thous_forms.most_common()),
    "bare_parenthesised_thousands": dict(bare_paren_thous.most_common()),
    "roman_numerals_adjacent_to_thousands_device": dict(adjacent_numeral.most_common(40)),
    "dot_below_top_tokens": dict(dot_below_tokens.most_common(30)),
    "sample_contexts": [c for c, _ in thous_ctx.most_common(25)],
}
p = REPO/"data"/"derived"/"phase2_crossref.json"
p.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")

print("=== THE THREE OVERLINE/LOWLINE SIGHTINGS, IN FULL ===")
for s in sightings:
    print(f"\n  {s['record_id']}  {s['mark']}  base={s['base']!r}  province={s['province']}")
    print(f"    …{s['context']}…")
print(f"\n=== THOUSANDS DEVICE: {sum(thous_forms.values()):,} occurrences, {len(thous_forms)} forms ===")
for f, n in thous_forms.most_common(20): print(f"  {f:<24}{n:>7,}")
print(f"\nbare (no pipe): {sum(bare_paren_thous.values()):,}")
for f, n in bare_paren_thous.most_common(8): print(f"  {f:<24}{n:>7,}")
print(f"\n=== ROMAN NUMERALS ADJACENT TO THE DEVICE ({sum(adjacent_numeral.values()):,}) ===")
for tok, n in adjacent_numeral.most_common(20): print(f"  {tok:<14}{n:>6,}   (= {roman_val(tok):,})")
print("\n=== SAMPLE CONTEXTS ===")
for c, _ in thous_ctx.most_common(10): print("  …", c.replace("\n"," "), "…")
print("\n=== U+0323 COMBINING DOT BELOW: top tokens ===")
for tok, n in dot_below_tokens.most_common(15): print(f"  {tok!r:<30}{n:>5}")
print("\nwritten", p)
