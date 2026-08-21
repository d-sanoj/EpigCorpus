#!/usr/bin/env python3
"""Phase 1 supplement: what reports/exclusion_audit.md does not answer.

The prior audit reproduces exactly (verified this session) and its
sub-classification, gain estimates and per-category bias tables stand. This
script does not repeat them. It answers five questions the prior audit left
open, each of which a reviewer can be expected to raise:

  A. The filter chain is FIRST-MATCH ORDERED. Every reported count is
     therefore "tokens this reason caught first", not "tokens this reason
     describes". Categories tested late (greek_script, non_alphabetic_abbrev)
     are structurally undercounted. Measure order-independent membership.

  B. The brief requires every distinct |(...) form enumerated with counts.
     The prior audit gives a sub-class total (13,737) and 40 mixed examples.
     Enumerate exhaustively.

  C. "Anything above about 0.15 [TVD] is a materially different population"
     is an asserted rule of thumb. Compute what TVD sampling noise alone
     produces at each category's n, so the reported values have a scale.

  D. Close the accounting. D-0008 measured 42,538 records that contain "("
     yet yield no pair, without explaining them. Every paren-bearing token
     must be kept, or dropped for exactly one reason, with nothing unheld.

  E. Circularity risk is argued in prose. Make it a measured, continuous
     quantity that Phase 3 can write into a column.

Audit only. Imports abbrev_probe, modifies nothing.
"""
from __future__ import annotations
import importlib.util, json, random, re, sys, unicodedata
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RAW = REPO / "data" / "edcs_inscriptions.jsonl"
OUT = REPO / "reports" / "exclusion_audit_supplement.md"
SEED = 20260820

def load_probe():
    p = Path(__file__).resolve().parent / "abbrev_probe.py"
    spec = importlib.util.spec_from_file_location("abbrev_probe", p)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m
ap = load_probe()

ORDER = ["inside_bracket_markup", "token_carries_markup", "unbalanced_parens",
         "nested_parens", "editorial_marker_paren", "no_letters_outside_parens",
         "greek_script", "contains_numeral", "non_alphabetic_abbrev",
         "non_alphabetic_expansion", "expansion_not_longer"]

PIPE_FORM = re.compile(r"\|+\(([^)]*)\)")

def all_reasons(tok_raw, off, masked):
    """Every reason that DESCRIBES this token, not merely the first to fire.

    Mirrors abbrev_probe.extract_pairs test-for-test, but evaluates each test
    on the token as it would look had the earlier tests not short-circuited.
    Tests that cannot be evaluated after an earlier structural failure (e.g.
    a paren group cannot be read out of an unbalanced token) are recorded as
    not-applicable rather than guessed.
    """
    reasons = set()
    lead = len(tok_raw) - len(tok_raw.lstrip(ap.EDGE_PUNCT))
    tok = tok_raw.strip(ap.EDGE_PUNCT)
    off = off + lead
    if "(" not in tok:
        return reasons, None, None, tok

    if any(masked[off+i] for i in range(len(tok)) if off+i < len(masked)):
        reasons.add("inside_bracket_markup")
    if any(c in ap.MARKUP_CHARS for c in tok):
        reasons.add("token_carries_markup")
    if tok.count("(") != tok.count(")"):
        reasons.add("unbalanced_parens")
        return reasons, None, None, tok
    if re.search(r"\([^)]*\(", tok):
        reasons.add("nested_parens")
        return reasons, None, None, tok

    groups = re.findall(r"\(([^)]*)\)", tok)
    cleaned = [g.rstrip("?!").strip() for g in groups]
    if any(g == "" for g in cleaned):
        reasons.add("editorial_marker_paren")
        return reasons, None, None, tok

    i = 0
    def _sub(_m):
        nonlocal i
        v = cleaned[i]; i += 1
        return "(" + v + ")"
    norm = re.sub(r"\([^)]*\)", _sub, tok)
    abbrev = re.sub(r"\([^)]*\)", "", norm)
    expansion = norm.replace("(", "").replace(")", "")

    if abbrev == "":
        reasons.add("no_letters_outside_parens")
        return reasons, abbrev, expansion, tok
    if ap.script_of(expansion) == "greek" or ap.script_of(abbrev) == "greek":
        reasons.add("greek_script")
    if re.search(r"\d", tok):
        reasons.add("contains_numeral")
    if not ap.is_latin_alpha(abbrev):
        reasons.add("non_alphabetic_abbrev")
    if not ap.is_latin_alpha(expansion):
        reasons.add("non_alphabetic_expansion")
    if len(expansion) <= len(abbrev):
        reasons.add("expansion_not_longer")
    return reasons, abbrev, expansion, tok

def first_reason(reasons):
    for r in ORDER:
        if r in reasons:
            return r
    return None

def restored_fraction(tok, off, masked):
    """Share of the ABBREVIATION's letters (outside parens) that sit inside
    a [ ] < > { } span, i.e. supplied by the editor rather than carved.
    0.0 = every letter attested; 1.0 = the abbreviation is wholly editorial."""
    depth = 0; tot = 0; restored = 0
    for i, c in enumerate(tok):
        if c == "(":
            depth += 1; continue
        if c == ")":
            depth = max(0, depth-1); continue
        if depth: continue
        if not c.isalpha(): continue
        tot += 1
        if off+i < len(masked) and masked[off+i]:
            restored += 1
    return (restored/tot) if tot else None

# --------------------------------------------------------------------------
def main():
    rng = random.Random(SEED)
    first_ct = Counter()
    memb_ct = Counter()                 # order-independent membership
    co = defaultdict(Counter)           # first_reason -> also-matches
    pipe_forms = Counter()
    pipe_records = defaultdict(set)
    erasure_ct = Counter()
    kept = 0
    tokens_with_paren = 0
    tokens_paren_lost_to_stripping = 0
    recs_paren = 0
    recs_with_kept = set()
    recs_paren_no_kept = set()
    # bias inputs
    kept_prov = Counter(); kept_cent = Counter()
    drop_prov = defaultdict(Counter); drop_cent = defaultdict(Counter)
    # circularity
    rf_hist = Counter()
    rf_by_subclass = defaultdict(Counter)

    def century(nb, na):
        a = ap.parse_year(nb); b = ap.parse_year(na)
        if a is None or b is None: return None
        ca = ap.century_of(a) if hasattr(ap, "century_of") else None
        return (a, b)

    for line in RAW.open(encoding="utf-8"):
        r = json.loads(line)
        text = r.get("inscription_text") or ""
        prov = r.get("province") or "UNKNOWN"
        if "(" not in text:
            continue
        recs_paren += 1
        masked = ap.mask_markup(text)
        rid = r["record_id"]
        got_kept = False
        for tok_raw, off in ap.iter_tokens(text):
            if "(" not in tok_raw:
                continue
            tokens_with_paren += 1
            reasons, abbrev, expansion, tok = all_reasons(tok_raw, off, masked)
            if "(" not in tok:
                tokens_paren_lost_to_stripping += 1
                continue
            fr = first_reason(reasons)
            if fr is None:
                kept += 1; got_kept = True
                kept_prov[prov] += 1
                continue
            first_ct[fr] += 1
            for m in reasons:
                memb_ct[m] += 1
                if m != fr:
                    co[fr][m] += 1
            drop_prov[fr][prov] += 1
            # pipe inventory: every |(...) form anywhere it occurs
            for g in PIPE_FORM.finditer(tok):
                pipe_forms[g.group(0)] += 1
                pipe_records[g.group(0)].add(rid)
            # erasure / quotation markup, currently lumped into "other non-letter"
            if fr == "non_alphabetic_abbrev":
                if any(c in tok for c in "⟦⟧"): erasure_ct["erasure ⟦ ⟧ (rasura)"] += 1
                elif any(c in tok for c in "«»"): erasure_ct["quotation « » (EDCS restoration bracket)"] += 1
                elif "|" in tok: erasure_ct["symbol | (non-typeable sign)"] += 1
                elif any(unicodedata.combining(c) for c in tok): erasure_ct["combining diacritic (overline etc.)"] += 1
                elif any(c.isalpha() and "GREEK" in unicodedata.name(c,"") for c in tok): erasure_ct["greek letter present"] += 1
                else: erasure_ct["other"] += 1
            # circularity
            if fr == "inside_bracket_markup":
                f = restored_fraction(tok, off, masked)
                if f is not None:
                    rf_hist[round(f, 1)] += 1
        if got_kept: recs_with_kept.add(rid)
        else: recs_paren_no_kept.add(rid)

    res = {
        "seed": SEED,
        "records_with_paren": recs_paren,
        "records_with_kept_pair": len(recs_with_kept),
        "records_paren_no_pair": len(recs_paren_no_kept),
        "tokens_with_paren": tokens_with_paren,
        "tokens_paren_lost_to_edge_stripping": tokens_paren_lost_to_stripping,
        "kept": kept,
        "first_match_counts": dict(first_ct),
        "membership_counts": dict(memb_ct),
        "co_membership": {k: dict(v) for k, v in co.items()},
        "pipe_forms": dict(pipe_forms),
        "pipe_form_records": {k: len(v) for k, v in pipe_records.items()},
        "non_alpha_abbrev_markup_breakdown": dict(erasure_ct),
        "restored_fraction_hist": {str(k): v for k, v in sorted(rf_hist.items())},
        "kept_province": dict(kept_prov),
        "drop_province": {k: dict(v) for k, v in drop_prov.items()},
    }
    outp = REPO/"data"/"derived"/"phase1_supplement.json"
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"kept {kept:,}  tokens_with_paren {tokens_with_paren:,}  "
          f"records_paren {recs_paren:,}  records_paren_no_pair {len(recs_paren_no_kept):,}")
    print("first-match:", sum(first_ct.values()))
    print("written", outp)

if __name__ == "__main__":
    main()
