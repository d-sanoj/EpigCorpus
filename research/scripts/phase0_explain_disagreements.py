import csv, json, re, unicodedata
from collections import Counter
from pathlib import Path
src=Path("/private/tmp/claude-501/-Users-sanoj-Documents-Projects-EpigCorpus/1f6e66e9-4419-4915-ac21-9f93dc989cbe/scratchpad/p0_second_impl.py").read_text()
ns={}; exec(src.split("n = 0")[0], ns); pairs_from_text=ns["pairs_from_text"]
REPO=Path("/Users/sanoj/Documents/Projects/EpigCorpus")
theirs=Counter()
with (REPO/"data"/"derived"/"abbrev_pairs.tsv").open(encoding="utf-8",newline="") as fh:
    rd=csv.reader(fh,delimiter="\t",quoting=csv.QUOTE_NONE); next(rd)
    for row in rd: theirs[(row[0],row[1],row[2])]+=1
mine=Counter()
for line in (REPO/"data"/"edcs_inscriptions.jsonl").open(encoding="utf-8"):
    r=json.loads(line)
    for a,e in pairs_from_text(r.get("inscription_text") or ""): mine[(r["record_id"],a,e)]+=1
only_t=list((theirs-mine).elements())
only_m_by_rec={}
for (rid,a,e) in (mine-theirs).elements(): only_m_by_rec.setdefault(rid,[]).append((a,e))

def strip_punct(s):
    return "".join(c for c in s if unicodedata.category(c)[0] in "LMN")

cls=Counter(); unexplained=[]
for (rid,a,e) in only_t:
    cands=only_m_by_rec.get(rid,[])
    if any(strip_punct(ma)==strip_punct(a) and strip_punct(me)==strip_punct(e) for ma,me in cands):
        cls["same pair, my version kept punctuation/diacritic noise"]+=1
    elif any(strip_punct(ma).rstrip("?!")==strip_punct(a) for ma,me in cands):
        cls["same pair, ?/! stripping difference"]+=1
    elif not cands:
        cls["my version produced nothing for this record (my masking too aggressive)"]+=1
    else:
        cls["unexplained"]+=1; unexplained.append((rid,a,e,cands[:4]))
print(f"only-in-primary disagreements: {len(only_t)}")
for k,v in cls.most_common(): print(f"  {v:>4}  {k}")
print()
for u in unexplained[:20]: print("  UNEXPLAINED", u)
