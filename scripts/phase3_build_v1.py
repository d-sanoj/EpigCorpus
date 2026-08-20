#!/usr/bin/env python3
"""Phase 3: reversible corrections -> data/derived/v1/abbrev_pairs_v1.tsv

Nothing is deleted and nothing is overwritten. The original `abbrev` and
`expansion` columns are carried through untouched; every correction is an
ADDED column. Filtering is by flag (`excluded_reason`), never by removal (R4).
Anything the explicit rules do not reach is UNRESOLVED, never imputed (R5).

Columns added
  abbrev_collapsed     geminatio run reduced to one letter -- BOTH keys kept
                       so the effect on ambiguity is measurable in both
                       directions (3a)
  corrected_expansion  the expansion after correction; == expansion when none
  correction_type      NONE | GEMINATIO_COLLAPSE | NUMERAL_TYPE1 |
                       NUMERAL_TYPE2 | NUMERAL_TYPE3
  plurality_flag       PLURAL (geminatio) | UNKNOWN
  geminatio_marker     the doubled run itself (DD, CCC, AUGG ...)
  numeral_class        NONE | TYPE1_NUMERAL_WORD | TYPE2_NUMERAL_ELLIPSIS |
                       TYPE3_NUMERAL_PREFIX | UNRESOLVED_NUMERAL
  numeral_value        arabic value of the numeral, Type 1-3 only
  supplied_word        Type 2 only: the unit word, stored SEPARATELY from the
                       numeral because it is not on the stone (D-0017)
  normalized_form      Latin reading of a Type 3 compound (sevir). NEVER a
                       gold label -- a gold label must not rest on a
                       contested scholarly reading.
  confidence           DECISIVE | THIN | UNRESOLVED | NA
  unresolved_reason    why a rule declined to decide
  excluded_reason      why this row must not enter the abbreviation task
  circularity_risk     share of the abbreviation's letters supplied by the
                       editor (D-0011)
  date_flag            POST700 | IMPLAUSIBLE | NONE  (3d)
  linebreak_fragment   the artifact discovered in D-0018
"""
from __future__ import annotations
import csv, importlib.util, json, re, sys
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
IN_TSV = REPO/"data"/"derived"/"abbrev_pairs.tsv"
OUT_DIR = REPO/"data"/"derived"/"v1"
OUT_TSV = OUT_DIR/"abbrev_pairs_v1.tsv"
STATS = OUT_DIR/"phase3_stats.json"
SEED = 20260820

def _load(name, path):
    s = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
WL = _load("wl", REPO/"scripts"/"phase3_wordlists.py")

# ---------------------------------------------------------------------------
# 3d. Dates. The brief forbids dropping post-700 records: prior work
# established that most are genuine early-medieval Christian inscriptions.
# Only DEMONSTRABLY mis-keyed dates are flagged, and the evidence for each is
# recorded here rather than left implicit. Every other late record keeps its
# date and stays in the release.
#
# Two records survive the evidence test. Both are flagged, neither is removed.
MISKEYED = {
    # Text is Constantinian tetrarchic titulature -- "d(omini) n(ostri)
    # Fl(avi) Val(eri)" -- which cannot be 1998 AD. Almost certainly a keying
    # slip for a 3rd-4th century year.
    "EDCS-27500083": "text reads d(omini) n(ostri) Fl(avi) Val(eri): tetrarchic, not 1998 AD",
    # Range 121-1125 spans 1004 years. The text is a Domitia Lucilla brick
    # stamp ("O d(oliare) d(e) f(iglinis) D(omitiae)"), a tightly dated
    # Hadrianic type. 1125 is 125 with a leading 1.
    "EDCS-30400458": "121-1125 spans 1004 years; Domitia Lucilla brick stamp is Hadrianic, so not_after is 125 mis-keyed as 1125",
}

ROMAN_UP = re.compile(r"^[IVXLCDM]+$")
# EDCS marks the plural by doubling the FINAL letter of the abbreviation:
#   Aug -> Augg(ustorum), Imp -> Impp(eratoribus), Cos -> Coss(ulibus),
#   Nob -> Nobb(ilissimis), Caes -> Caess(aribus).
# For a one-letter abbreviation the doubled letter is both first and last:
#   D -> DD(ominis), N -> NN(ostris). A leading-run rule catches only the
#   second family and misses AUGG/IMPP/CONSS entirely.
TRAILING_RUN = re.compile(r"(([A-Za-z])\2+)$")
ANY_RUN = re.compile(r"(([A-Za-z])\2+)")
ROMAN_VAL = {"I":1,"V":5,"X":10,"L":50,"C":100,"D":500,"M":1000}

def roman_value(s):
    t = 0; prev = 0
    for c in reversed(s.upper()):
        v = ROMAN_VAL.get(c, 0)
        t += -v if v < prev else v
        prev = max(prev, v)
    return t

def trailing_run(ab):
    m = TRAILING_RUN.search(ab)
    return m.group(1) if m else ""

def has_any_run(ab):
    return bool(ANY_RUN.search(ab))

def collapse(ab, ex):
    """Reduce the trailing doubled run to ONE letter, in abbrev and expansion.

    Augg + ustorum -> Aug + ustorum = Augustorum
    DD   + ominis  -> D   + ominis  = Dominis
    Returns (ab, ex) unchanged when there is no run, or when the expansion
    does not begin with the abbreviation (multi-group tokens such as
    co(n)s(ul), where the arithmetic does not apply). Never guesses.
    """
    run = trailing_run(ab)
    if not run:
        return ab, ex, ""
    if not ex.startswith(ab):
        return ab, ex, run          # marker recorded, no collapse attempted
    drop = len(run) - 1
    ab2 = ab[:-drop]
    ex2 = ab2 + ex[len(ab):]
    return ab2, ex2, run

# --------------------------------------------------------------------------
def main():
    rows = []
    with IN_TSV.open(encoding="utf-8", newline="") as fh:
        rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
        header = next(rd)
        for r in rd:
            rows.append(r)
    print(f"read {len(rows):,} pairs", file=sys.stderr)

    # ---- control lexicon: expansions from NON-suspect tokens only, so the
    #      arbitration cannot feed on the very forms it is judging.
    def numeral_rest(ab, ex):
        """The paren content when the token has the numeral-fusion shape."""
        if ROMAN_UP.match(ab) and ex.startswith(ab) and len(ex) > len(ab):
            return ex[len(ab):].lower()
        return None

    ALL_LISTS = (WL.TYPE1_NUMERAL_WORD | WL.TYPE2_SUPPLIED_UNIT
                 | WL.TYPE3_NUMERAL_PREFIX)

    # The control lexicon must not contain the readings it is asked to judge.
    # Excluded: any token with a leading doubled run, and any token whose
    # remainder after a numeral prefix is on an explicit word list. The second
    # exclusion is the trap dd_diagnostic.md documented -- X(milia) is why
    # "xmilia" looked attested, which made an earlier arbitration mis-rule
    # XX(milia) as geminatio. Ordinary abbreviations that merely start with a
    # numeral LETTER (M(anibus), D(is), C(aius)) stay in: they are the bulk of
    # real Latin and removing them would cripple the lexicon.
    control = Counter()
    for r in rows:
        ab, ex = r[1], r[2]
        if has_any_run(ab):
            continue
        nr = numeral_rest(ab, ex)
        if nr is not None and nr in ALL_LISTS:
            continue
        control[ex.lower()] += 1
    print(f"control lexicon: {len(control):,} distinct expansions", file=sys.stderr)

    stats = Counter()
    ctype_ct = Counter(); nclass_ct = Counter(); conf_ct = Counter()
    unresolved_ct = Counter(); excluded_ct = Counter(); dateflag_ct = Counter()
    gem_forms = Counter(); type1_hit = Counter(); type2_hit = Counter(); type3_hit = Counter()
    numeral_unresolved = Counter()
    single_numeral_resolved = Counter()
    date_evidence = []
    lb_examples = []

    out_header = header + [
        "abbrev_collapsed", "corrected_expansion", "correction_type",
        "plurality_flag", "geminatio_marker", "numeral_class", "numeral_value",
        "supplied_word", "normalized_form", "confidence", "unresolved_reason",
        "excluded_reason", "circularity_risk", "date_flag", "linebreak_fragment",
    ]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fout = OUT_TSV.open("w", encoding="utf-8", newline="")
    w = csv.writer(fout, delimiter="\t", quoting=csv.QUOTE_NONE,
                   escapechar="\\", lineterminator="\n")
    w.writerow(out_header)

    for r in rows:
        rid, ab, ex, lctx, rctx, prov, dfrom, dto = r
        ab_c, ex_c, run = collapse(ab, ex)
        collapsible = (run != "" and ex_c != ex)

        corrected = ex
        ctype = "NONE"
        plural = "UNKNOWN"
        nclass = "NONE"
        nval = ""
        supplied = ""
        norm = ""
        conf = "NA"
        ureason = ""
        excl = ""

        # ---------- 3b/3c numeral classification, by EXPLICIT LIST ---------
        # Runs FIRST. The word lists are explicit and auditable; the geminatio
        # arbitration is statistical and can be fooled by exactly these forms.
        # A single-character numeral (X, V, L, C, D, I, M) is treated as a
        # numeral ONLY if its paren content is on a list. That is the
        # measurable replacement for the eyeballed "~four-fifths" of
        # dd_diagnostic 6b: everything else stays an ordinary abbreviation.
        nrest = None
        if ROMAN_UP.match(ab) and ex.startswith(ab) and len(ex) > len(ab):
            nrest = ex[len(ab):]
        numeral_decided = False
        if nrest is not None:
            rl = nrest.lower()
            if rl in WL.TYPE1_NUMERAL_WORD:
                nclass = "TYPE1_NUMERAL_WORD"; ctype = "NUMERAL_TYPE1"
                corrected = nrest; nval = str(roman_value(ab)); conf = "DECISIVE"
                type1_hit[rl] += 1; numeral_decided = True
            elif rl in WL.TYPE2_SUPPLIED_UNIT:
                nclass = "TYPE2_NUMERAL_ELLIPSIS"; ctype = "NUMERAL_TYPE2"
                corrected = nrest; supplied = nrest; nval = str(roman_value(ab))
                conf = "DECISIVE"; excl = "NUMERAL_ELLIPSIS_NOT_AN_ABBREVIATION"
                type2_hit[rl] += 1; numeral_decided = True
            elif rl in WL.TYPE3_NUMERAL_PREFIX:
                nclass = "TYPE3_NUMERAL_PREFIX"; ctype = "NUMERAL_TYPE3"
                corrected = ex                      # gold = EDCS surface form
                nval = str(roman_value(ab)); conf = "DECISIVE"
                norm = WL.TYPE3_OFFICE_READING.get((ab.upper(), rl), "")
                if not norm:
                    pre = WL.TYPE3_NORMALISED.get(ab.upper(), "")
                    norm = (pre + rl) if pre else ""
                type3_hit[rl] += 1; numeral_decided = True
            if numeral_decided and len(ab) == 1:
                single_numeral_resolved[nclass] += 1

        # ---------- 3a geminatio arbitration -------------------------------
        if collapsible and not numeral_decided:
            naive_s = control.get(ex.lower(), 0)
            coll_s = control.get(ex_c.lower(), 0)
            if coll_s == 0 and naive_s == 0:
                conf = "UNRESOLVED"; ureason = "geminatio: neither reading attested in control lexicon"
            elif coll_s >= 3*max(naive_s, 1) and coll_s > 0:
                corrected = ex_c; ctype = "GEMINATIO_COLLAPSE"; plural = "PLURAL"
                conf = "DECISIVE"; gem_forms[ab.lower()] += 1
            elif naive_s >= 3*max(coll_s, 1) and naive_s > 0:
                ctype = "NONE"; conf = "DECISIVE"          # correct as is
            elif coll_s > naive_s:
                corrected = ex_c; ctype = "GEMINATIO_COLLAPSE"; plural = "PLURAL"
                conf = "THIN"; gem_forms[ab.lower()] += 1
            elif naive_s > 0:
                ctype = "NONE"; conf = "THIN"
            else:
                conf = "UNRESOLVED"; ureason = "geminatio: tie with no support"
        elif run and not numeral_decided and not collapsible:
            conf = "UNRESOLVED"
            ureason = "geminatio: doubled run present but expansion does not begin with the abbreviation"

        # ---------- what the explicit rules could not reach -----------------
        # A MULTI-character numeral prefix cannot be a praenomen initial, so if
        # no list and no geminatio verdict reached it, it is genuinely
        # unresolved. A single-character one is just an ordinary abbreviation.
        if (nrest is not None and not numeral_decided and len(ab) > 1
                and conf in ("NA", "UNRESOLVED")):
            nclass = "UNRESOLVED_NUMERAL"; conf = "UNRESOLVED"
            ureason = f"numeral prefix, paren content '{nrest.lower()}' in no word list"
            numeral_unresolved[nrest.lower()] += 1

        # ---------- circularity (D-0011) -----------------------------------
        # Every KEPT pair sits wholly outside [ ] < > { } by construction of
        # abbrev_probe, so no abbreviation letter here is editor-supplied.
        circ = "0.0"

        # ---------- 3d dates ------------------------------------------------
        dflag = "NONE"
        yto = None
        try:
            yto = int(dto) if dto not in ("", "-") else None
        except ValueError:
            yto = None
        if yto is not None:
            if rid.rsplit("-",1)[0] in MISKEYED:
                dflag = "MISKEYED"
            elif yto > 1000:
                dflag = "LATE_OVER_1000"
                if len(date_evidence) < 400:
                    date_evidence.append({"record": rid, "date_from": dfrom,
                                          "date_to": dto, "abbrev": ab,
                                          "expansion": ex, "province": prov,
                                          "context": (lctx + " [" + ab + "] " + rctx)[:200]})
            elif yto > 700:
                dflag = "POST700"

        # ---------- line-break fragmentation (D-0018) -----------------------
        lb = "0"
        l = lctx.rstrip()
        if l.endswith("/") and len(l) >= 2 and l[-2].isalpha():
            lb = "1"
            if len(lb_examples) < 200:
                lb_examples.append({"record": rid, "abbrev": ab, "expansion": ex,
                                    "left_context": lctx, "right_context": rctx})

        ctype_ct[ctype] += 1; nclass_ct[nclass] += 1; conf_ct[conf] += 1
        if ureason: unresolved_ct[ureason.split(":")[0]] += 1
        if excl: excluded_ct[excl] += 1
        dateflag_ct[dflag] += 1
        if lb == "1": stats["linebreak_fragment"] += 1

        w.writerow([rid, ab, ex, lctx, rctx, prov, dfrom, dto,
                    ab_c, corrected, ctype, plural, run, nclass, nval,
                    supplied, norm, conf, ureason, excl, circ, dflag, lb])
    fout.close()

    out = {
      "seed": SEED,
      "input_pairs": len(rows),
      "control_lexicon_size": len(control),
      "correction_type": dict(ctype_ct),
      "numeral_class": dict(nclass_ct),
      "confidence": dict(conf_ct),
      "excluded_reason": dict(excluded_ct),
      "unresolved_reason": dict(unresolved_ct),
      "date_flag": dict(dateflag_ct),
      "linebreak_fragment": stats["linebreak_fragment"],
      "geminatio_forms": dict(gem_forms.most_common(60)),
      "type1_hits": dict(type1_hit.most_common()),
      "type2_hits": dict(type2_hit.most_common()),
      "type3_hits": dict(type3_hit.most_common()),
      "numeral_unresolved": dict(numeral_unresolved.most_common(60)),
      "single_char_numeral_resolved": dict(single_numeral_resolved),
      "implausible_date_evidence": date_evidence,
      "linebreak_examples": lb_examples[:40],
    }
    STATS.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"\nwrote {OUT_TSV}")
    print("correction_type :", dict(ctype_ct))
    print("numeral_class   :", dict(nclass_ct))
    print("confidence      :", dict(conf_ct))
    print("excluded        :", dict(excluded_ct))
    print("date_flag       :", dict(dateflag_ct))
    print("linebreak frag  :", stats["linebreak_fragment"])

if __name__ == "__main__":
    main()
