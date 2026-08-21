# [ACTION REQUIRED — HUMAN] EDCS redistribution permission

**Status: UNRESOLVED. This blocks release of the derived dataset.**

The dataset in `data/derived/v1/` is built from 588,509 records harvested from
the Epigraphik-Datenbank Clauss-Slaby (EDCS). Publishing the derived tables —
even though they contain only extracted (abbreviation, expansion) pairs and
short context windows rather than whole inscription texts — redistributes
material EDCS compiled. Permission has **not** been sought or granted.

**What is safe to publish without permission:** the code, the reports, the
measured statistics (counts, distributions, model results). Those are our own
observations *about* the database.

**What is not:** `abbrev_pairs_v1.tsv`, the splits, and the `|(...)` inventory,
all of which contain EDCS text.

Nothing should be released publicly until this is answered in writing.

---

## Draft email — review before sending

> **To:** the EDCS maintainers (Manfred Clauss / Anne Kolb, via the contact
> address on the EDCS site — **[VERIFY: confirm the current contact address
> before sending]**)
>
> **Subject:** Permission request — redistribution of derived data from EDCS
>
> Dear Professor Clauss and colleagues,
>
> I am preparing an academic publication that uses the Epigraphik-Datenbank
> Clauss-Slaby as its source. I am writing to ask permission before releasing
> any derived data, and to check that what I plan to publish is acceptable to
> you.
>
> **What the work does.** EDCS marks the editorial expansion of ancient
> abbreviations in round parentheses — `D(is) M(anibus)`. I have used those
> parentheses as labels for the task of automatically expanding Latin
> epigraphic abbreviations, extracting 1,424,314 (abbreviation, expansion)
> pairs from 588,509 records. The paper is mainly a study of EDCS's
> transcription conventions and how they affect anyone building datasets from
> the database — for example, that the numeral vinculum is not preserved in
> the plain-text field, and that the `|` character stands for at least eight
> distinct epigraphic signs.
>
> **What I would like to release.** A derived table of the extracted pairs.
> Each row holds an abbreviation, its expansion, a context window of about 40
> characters either side, the EDCS record identifier, province, and date
> range. It does **not** contain full inscription texts, and it is not usable
> as a substitute for EDCS itself — it is a training resource for a single
> narrow task, and every row points back to your record id.
>
> **What I am asking.**
> 1. Do you permit redistribution of such a derived table under an open
>    licence for academic use? If so, is there a licence you prefer?
> 2. How would you like EDCS cited?
> 3. Are there restrictions I should observe — on context length, on record
>    counts, or on particular fields?
> 4. If redistribution is not acceptable, would you permit release of the
>    extraction *code* alone, so that others can rebuild the table from EDCS
>    themselves?
>
> I am happy to share the manuscript and the derived data with you before
> publication, and to make any changes you ask for. If it would be useful, I
> can also send the list of transcription-convention observations separately —
> several may be of interest to you independently of my paper.
>
> The harvest was carried out with rate limiting and without circumventing any
> access control. If the rate or method caused any problem on your side, I
> would like to know so I can correct it.
>
> With thanks for the database, which has been the foundation of this work.
>
> Yours sincerely,
> **[your name, affiliation, and contact address]**

---

## Before sending — checklist

- [ ] **[VERIFY]** Confirm the current EDCS contact address from the official site
- [ ] Insert your name, affiliation and contact details
- [ ] Decide whether to offer the manuscript pre-publication (recommended)
- [ ] Record the date sent here: ______________
- [ ] Record the reply and its terms here: ______________

## If permission is refused or unanswered

Fallback: release **code and reports only**, with a script that rebuilds the
derived tables from a user's own EDCS harvest. The paper's findings survive
intact — they are observations about the database, not the database itself.
The `|(...)` inventory would have to be published as counts and sign families
without the underlying text, which weakens it but does not destroy it.
