"""Phase 0 adversarial check: a SECOND, independently written extractor.

Not a copy of scripts/abbrev_probe.py. Written directly from the EDCS
convention (round parens = editorial expansion of an ancient abbreviation;
[] <> {} are other Leiden markup; / is a line break). Purpose: catch a
coding bug in the primary extractor. It cannot catch a shared
misunderstanding of the convention -- that is Phase 1's job.
"""
import csv, json, re
from collections import Counter
from pathlib import Path

REPO = Path("/Users/sanoj/Documents/Projects/EpigCorpus")

# mask non-round Leiden markup as spans; unclosed opener masks to end
def mask(t):
    out = []
    depth_sq = depth_ang = depth_cur = 0
    for ch in t:
        if ch == "[": depth_sq += 1; out.append(" "); continue
        if ch == "]": depth_sq = max(0, depth_sq-1); out.append(" "); continue
        if ch == "<": depth_ang += 1; out.append(" "); continue
        if ch == ">": depth_ang = max(0, depth_ang-1); out.append(" "); continue
        if ch == "{": depth_cur += 1; out.append(" "); continue
        if ch == "}": depth_cur = max(0, depth_cur-1); out.append(" "); continue
        out.append("\x00" if (depth_sq or depth_ang or depth_cur) else ch)
    s = "".join(out)
    # a ']' with no opener masks from the start: emulate by second pass
    return s

CLOSE_NO_OPEN = re.compile(r"^[^\[]*\]")

def tokens(t):
    # '/' is a hard token boundary, not a delimiter pair
    return re.split(r"[\s/]+", t)

LETTER = re.compile(r"[A-Za-zÀ-ɏ]")
PAREN = re.compile(r"\(([^()]*)\)")

def pairs_from_text(text):
    if "]" in text and "[" in text:
        pre, _, rest = text.partition("]")
        if "[" not in pre:
            text = " "*len(pre+"]") + rest
    m = mask(text)
    res = []
    for tok in tokens(m):
        if "\x00" in tok:            # touched bracket markup -> excluded
            continue
        if "(" not in tok or ")" not in tok:
            continue
        if tok.count("(") != tok.count(")"):
            continue
        inner = PAREN.findall(tok)
        if not inner:
            continue
        if any(("?" in i) or ("!" in i) for i in inner):
            inner2 = [i.rstrip("?!") for i in inner]
            if any(("?" in i) or ("!" in i) or not i for i in inner2):
                continue
            tok = PAREN.sub(lambda mo: "("+mo.group(1).rstrip("?!")+")", tok)
            inner = inner2
        outside = PAREN.sub("", tok)
        if not LETTER.search(outside):
            continue
        if not all(LETTER.search(i) for i in inner):
            continue
        abbrev = outside
        expansion = tok.replace("(","").replace(")","")
        res.append((abbrev, expansion))
    return res

n = 0
mine = Counter()
for line in (REPO/"data"/"edcs_inscriptions.jsonl").open(encoding="utf-8"):
    r = json.loads(line)
    for a,e in pairs_from_text(r.get("inscription_text") or ""):
        n += 1
        mine[(r["record_id"], a, e)] += 1

theirs = Counter()
with (REPO/"data"/"derived"/"abbrev_pairs.tsv").open(encoding="utf-8", newline="") as fh:
    rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
    next(rd)
    for row in rd:
        theirs[(row[0], row[1], row[2])] += 1

only_mine = mine - theirs
only_theirs = theirs - mine
print(f"second impl pairs       : {n:,}")
print(f"primary probe pairs     : {sum(theirs.values()):,}")
print(f"agreement (multiset ∩)  : {sum((mine & theirs).values()):,}")
print(f"only in second impl     : {sum(only_mine.values()):,}")
print(f"only in primary probe   : {sum(only_theirs.values()):,}")
print()
print("-- sample: only in second impl --")
for k,v in list(only_mine.items())[:15]: print("  ", k, v)
print("-- sample: only in primary probe --")
for k,v in list(only_theirs.items())[:15]: print("  ", k, v)
