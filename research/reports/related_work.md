# Related work

Anchors given in the brief and treated as established. Nothing is cited here
that was not supplied; where a claim needs a source I do not have, it is marked
**NEEDS CITATION** rather than attributed to a plausible-sounding paper.

## Anchors

| key | reference | relevance to this pipeline |
| --- | --- | --- |
| Assael et al. 2025 | *Aeneas*, Nature. Restores lost characters across ~176k inscriptions. | **Primary contrast.** Aeneas restores characters lost from the stone. This pipeline expands abbreviations the stone deliberately carries. Different task, different label source: a lacuna versus an editorial parenthesis. |
| Kase, Heřmánková & Sobotková 2021 | CHR. ML classification of EDCS. | Prior ML use of this exact corpus; establishes EDCS as a tractable ML source. |
| Heřmánková, Kase & Sobotková 2021 | J. Digital History. Inscriptions as data. | Framing for treating an epigraphic edition as a dataset with its own conventions. |
| Ballsun-Stanton, Heřmánková & Laurence 2024 | LatEpig. | The tool this pipeline must differentiate from. |
| Elliott 1998 | The standing abbreviation index. | The gap being filled: a static index versus a corpus-derived, frequency- and context-aware resource. |

## Step 0 notes

**D-0004 (segment-level records, group-level leakage).** Grouping records by
EDCS base id before any split is standard practice for corpora with
near-duplicate documents, but I have no citation in hand tying it to this
corpus specifically, and neither Kase et al. 2021 nor Heřmánková et al. 2021 is
quoted here as having addressed it — I have not read them in this session and
will not attribute a position to them.
**NEEDS CITATION:** whether prior ML work on EDCS (Kase et al. 2021) split by
`record_id` or by monument, and whether the 45,655 multi-segment records were
handled.
**NEEDS CITATION:** EDCS's own documentation of the `-N` record-id suffix.
