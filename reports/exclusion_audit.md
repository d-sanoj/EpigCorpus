# Exclusion audit

Every token `scripts/abbrev_probe.py` refused, re-derived and examined. Audit only: the probe is imported, not edited, and nothing under `src/` or `data/` is touched.

## 0. Does this reproduce the probe's decisions?

| reason | probe reported | this audit | status |
| --- | --- | --- | --- |
| inside_bracket_markup | 253,256 | 253,256 | match |
| editorial_marker_paren | 58,720 | 58,720 | match |
| non_alphabetic_abbrev | 16,335 | 16,335 | match |
| greek_script | 12,987 | 12,987 | match |
| unbalanced_parens | 561 | 561 | match |
| no_letters_outside_parens | 469 | 469 | match |
| token_carries_markup | 339 | 339 | match |
| non_alphabetic_expansion | 27 | 27 | match |
| contains_numeral | 19 | 19 | match |
| nested_parens | 1 | 1 | match |
| (kept pairs) | 1,424,314 | 1,424,314 | match |

**Every count matches, so the analysis below describes the real dataset.**

Total dropped: **342,714** against **1,424,314** kept (19.39% of all candidate tokens).

## 1. `inside_bracket_markup` — 253,256

Round parentheses inside `[ ]` are expansions of text the editor restored rather than text the stone carries.

**Sub-classification.** One label was hiding several situations.

| sub-class | count | share of category |
| --- | --- | --- |
| whole thing is editorial reconstruction | 149,582 | 59.06% |
| abbreviation partly restored, expansion on surviving text | 64,991 | 25.66% |
| abbreviation partly restored, expansion also inside the bracket | 22,612 | 8.93% |
| abbreviation restored, expansion outside the bracket | 9,359 | 3.70% |
| no alphabetic content (editorial mark inside a bracket) | 3,312 | 1.31% |
| expansion inside the bracket, no surviving abbreviation letters | 2,046 | 0.81% |
| abbreviation and expansion both straddle the bracket | 1,334 | 0.53% |
| expansion straddles the bracket, no abbreviation letters | 16 | 0.01% |
| abbreviation restored, expansion straddles the bracket | 4 | 0.00% |

**This overturns an assumption in the brief.** `[Imp(erator)` was offered as an example of an abbreviation left intact with only its surroundings bracketed. It is not: in `[Imp(erator) Caes]ar` the bracket span closes after *Caes*, so the letters *Imp* are themselves restored. The category where the abbreviation survives untouched and only its neighbours are bracketed is a different reason entirely — `token_carries_markup`, 339 tokens, section 7. Inside this category no sub-class has an intact abbreviation sitting outside the brackets.

**40 examples.**

| raw token | inscription id | sub-class | inscription_text |
| --- | --- | --- | --- |
| `[I(ovi)]` | EDCS-00000003-0 | whole thing is editorial reconstruction | [I(ovi)] O(ptimo) M(aximo) |
| `(!)` | EDCS-00000006-0 | no alphabetic content (editorial mark inside a bracket) | [3]ai stabulum <B=V>en[etae (!) 3] |
| `frumen]t(o)` | EDCS-00000009-0 | abbreviation partly restored, expansion on surviving text | [3 in] muro [3] / [3 qui frumen]t(o) publ(ico) i[ncisi sunt 3] |
| `[Imp(erator)` | EDCS-00000010-0 | whole thing is editorial reconstruction | [Imp(erator) Caes]ar D[ivi f(ilius) / Aug]ustus / [pontife]x maxim[us / tribuni]c(ia) potest(ate) [XVII / ex s(enatus) c(onsulto)] termin[avit] |
| `f(ilius)` | EDCS-00000010-0 | whole thing is editorial reconstruction | [Imp(erator) Caes]ar D[ivi f(ilius) / Aug]ustus / [pontife]x maxim[us / tribuni]c(ia) potest(ate) [XVII / ex s(enatus) c(onsulto)] termin[avit] |
| `tribuni]c(ia)` | EDCS-00000010-0 | abbreviation partly restored, expansion on surviving text | [Imp(erator) Caes]ar D[ivi f(ilius) / Aug]ustus / [pontife]x maxim[us / tribuni]c(ia) potest(ate) [XVII / ex s(enatus) c(onsulto)] termin[avit] |
| `s(enatus)` | EDCS-00000010-0 | whole thing is editorial reconstruction | [Imp(erator) Caes]ar D[ivi f(ilius) / Aug]ustus / [pontife]x maxim[us / tribuni]c(ia) potest(ate) [XVII / ex s(enatus) c(onsulto)] termin[avit] |
| `c(onsulto)]` | EDCS-00000010-0 | whole thing is editorial reconstruction | [Imp(erator) Caes]ar D[ivi f(ilius) / Aug]ustus / [pontife]x maxim[us / tribuni]c(ia) potest(ate) [XVII / ex s(enatus) c(onsulto)] termin[avit] |
| `[Ho]muncioni(s)` | EDCS-00000011-0 | abbreviation partly restored, expansion on surviving text | [Ho]muncioni(s) / PR() P() |
| `a]nn(os)` | EDCS-00000013-0 | abbreviation partly restored, expansion on surviving text | [3]i Exupera[nti 3] / [3 vixit a]nn(os) plus [minus 3] / [3 dep(osit)] VIII kal(endas) [3] / [3]niano Aug(usto) [3] / [ |
| `dep(osit)]` | EDCS-00000013-0 | whole thing is editorial reconstruction | [3]i Exupera[nti 3] / [3 vixit a]nn(os) plus [minus 3] / [3 dep(osit)] VIII kal(endas) [3] / [3]niano Aug(usto) [3] / [ |
| `c[ons(ule)]` | EDCS-00000015-0 | abbreviation partly restored, expansion also inside the bracket | ] / [3]L Pyrr[3] / [3]o prid(ie) idu[s 3] / [3] v(iro) c(larissimo) c[ons(ule)] |
| `de[p]os(i)t(us)` | EDCS-00000026-1 | abbreviation partly restored, expansion on surviving text | de[p]os(i)t(us) idi(bus) Mar(tiis) |
| `v(ir)` | EDCS-00000028-0 | whole thing is editorial reconstruction | [Salu]o d(omino) n(ostro) inv[icto semper rege Theoderico] / [3 v(ir) c(larissimus) et inl(lustris) praef(ectus) Vrb(i) (?) / [6] / [3 splen]dorem m[armor]um / [ad pristinum statum rev]ocaret aut vinceret / ca[ldari?] [3] thermarum [[M]] marin{ian}arum [3] ferro h[3] tubulo [3]l / ter repara[vit 3]  […] |
| `c(larissimus)` | EDCS-00000028-0 | whole thing is editorial reconstruction | [Salu]o d(omino) n(ostro) inv[icto semper rege Theoderico] / [3 v(ir) c(larissimus) et inl(lustris) praef(ectus) Vrb(i) (?) / [6] / [3 splen]dorem m[armor]um / [ad pristinum statum rev]ocaret aut vinceret / ca[ldari?] [3] thermarum [[M]] marin{ian}arum [3] ferro h[3] tubulo [3]l / ter repara[vit 3]  […] |
| `inl(lustris)` | EDCS-00000028-0 | whole thing is editorial reconstruction | [Salu]o d(omino) n(ostro) inv[icto semper rege Theoderico] / [3 v(ir) c(larissimus) et inl(lustris) praef(ectus) Vrb(i) (?) / [6] / [3 splen]dorem m[armor]um / [ad pristinum statum rev]ocaret aut vinceret / ca[ldari?] [3] thermarum [[M]] marin{ian}arum [3] ferro h[3] tubulo [3]l / ter repara[vit 3]  […] |
| `praef(ectus)` | EDCS-00000028-0 | whole thing is editorial reconstruction | [Salu]o d(omino) n(ostro) inv[icto semper rege Theoderico] / [3 v(ir) c(larissimus) et inl(lustris) praef(ectus) Vrb(i) (?) / [6] / [3 splen]dorem m[armor]um / [ad pristinum statum rev]ocaret aut vinceret / ca[ldari?] [3] thermarum [[M]] marin{ian}arum [3] ferro h[3] tubulo [3]l / ter repara[vit 3]  […] |
| `Vrb(i)` | EDCS-00000028-0 | whole thing is editorial reconstruction | [Salu]o d(omino) n(ostro) inv[icto semper rege Theoderico] / [3 v(ir) c(larissimus) et inl(lustris) praef(ectus) Vrb(i) (?) / [6] / [3 splen]dorem m[armor]um / [ad pristinum statum rev]ocaret aut vinceret / ca[ldari?] [3] thermarum [[M]] marin{ian}arum [3] ferro h[3] tubulo [3]l / ter repara[vit 3]  […] |
| `(?)` | EDCS-00000028-0 | no alphabetic content (editorial mark inside a bracket) | [Salu]o d(omino) n(ostro) inv[icto semper rege Theoderico] / [3 v(ir) c(larissimus) et inl(lustris) praef(ectus) Vrb(i) (?) / [6] / [3 splen]dorem m[armor]um / [ad pristinum statum rev]ocaret aut vinceret / ca[ldari?] [3] thermarum [[M]] marin{ian}arum [3] ferro h[3] tubulo [3]l / ter repara[vit 3]  […] |
| `v(iro)` | EDCS-00000028-0 | whole thing is editorial reconstruction | [Salu]o d(omino) n(ostro) inv[icto semper rege Theoderico] / [3 v(ir) c(larissimus) et inl(lustris) praef(ectus) Vrb(i) (?) / [6] / [3 splen]dorem m[armor]um / [ad pristinum statum rev]ocaret aut vinceret / ca[ldari?] [3] thermarum [[M]] marin{ian}arum [3] ferro h[3] tubulo [3]l / ter repara[vit 3]  […] |
| `c(larissimo)` | EDCS-00000028-0 | whole thing is editorial reconstruction | [Salu]o d(omino) n(ostro) inv[icto semper rege Theoderico] / [3 v(ir) c(larissimus) et inl(lustris) praef(ectus) Vrb(i) (?) / [6] / [3 splen]dorem m[armor]um / [ad pristinum statum rev]ocaret aut vinceret / ca[ldari?] [3] thermarum [[M]] marin{ian}arum [3] ferro h[3] tubulo [3]l / ter repara[vit 3]  […] |
| `August]al(is)` | EDCS-00000030-0 | abbreviation partly restored, expansion on surviving text | ] / [3 August]al(is) Puteolis / [et Miseni 3 ex d(ecreto)] d(ecurionum) sua pe<c=Q>(unia) fecit |
| `d(ecreto)]` | EDCS-00000030-0 | whole thing is editorial reconstruction | ] / [3 August]al(is) Puteolis / [et Miseni 3 ex d(ecreto)] d(ecurionum) sua pe<c=Q>(unia) fecit |
| `pe<c=Q>(unia)` | EDCS-00000030-0 | abbreviation partly restored, expansion on surviving text | ] / [3 August]al(is) Puteolis / [et Miseni 3 ex d(ecreto)] d(ecurionum) sua pe<c=Q>(unia) fecit |
| `[M(anibus)]` | EDCS-00000032-0 | whole thing is editorial reconstruction | D(is) [M(anibus)] / Numisiae Sec[undae] / M(arcus) Domitius / Abascantus / alumnae suae / [6] / [6] / [ |
| `du[lc(issimo)]` | EDCS-00000033-0 | abbreviation partly restored, expansion also inside the bracket | Thallioni / delicato suo / qui vixit ann(is) / XV du[lc(issimo)] /puero / [ |
| `Ti(?)]neio[s` | EDCS-00000039-0 | abbreviation partly restored, expansion on surviving text | [3 Ti(?)]neio[s 3] v(ir) c(larissimus) / [aedem Apollinis] Delfici in c[ampo iuvenu]m / [quam promise]rat sumpti[bus suis ex / ma]rm[ore restitu]it et [de]d[icavit 3] / [3]ia[no(?) 3 co(n)s(ulibus)] |
| `[3]ia[no(?)` | EDCS-00000039-0 | abbreviation partly restored, expansion on surviving text | [3 Ti(?)]neio[s 3] v(ir) c(larissimus) / [aedem Apollinis] Delfici in c[ampo iuvenu]m / [quam promise]rat sumpti[bus suis ex / ma]rm[ore restitu]it et [de]d[icavit 3] / [3]ia[no(?) 3 co(n)s(ulibus)] |
| `co(n)s(ulibus)]` | EDCS-00000039-0 | whole thing is editorial reconstruction | [3 Ti(?)]neio[s 3] v(ir) c(larissimus) / [aedem Apollinis] Delfici in c[ampo iuvenu]m / [quam promise]rat sumpti[bus suis ex / ma]rm[ore restitu]it et [de]d[icavit 3] / [3]ia[no(?) 3 co(n)s(ulibus)] |
| `[3(?)` | EDCS-00000047-0 | no alphabetic content (editorial mark inside a bracket) | [3(?) ex li]berali[tate Imp(eratoris) Caes(aris) (?)] M(arci) Anto[ni Gordiani / Pii Felicis Aug(usti) 3] |
| `Imp(eratoris)` | EDCS-00000047-0 | whole thing is editorial reconstruction | [3(?) ex li]berali[tate Imp(eratoris) Caes(aris) (?)] M(arci) Anto[ni Gordiani / Pii Felicis Aug(usti) 3] |
| `Caes(aris)` | EDCS-00000047-0 | whole thing is editorial reconstruction | [3(?) ex li]berali[tate Imp(eratoris) Caes(aris) (?)] M(arci) Anto[ni Gordiani / Pii Felicis Aug(usti) 3] |
| `(?)]` | EDCS-00000047-0 | no alphabetic content (editorial mark inside a bracket) | [3(?) ex li]berali[tate Imp(eratoris) Caes(aris) (?)] M(arci) Anto[ni Gordiani / Pii Felicis Aug(usti) 3] |
| `Aug(usti)` | EDCS-00000047-0 | whole thing is editorial reconstruction | [3(?) ex li]berali[tate Imp(eratoris) Caes(aris) (?)] M(arci) Anto[ni Gordiani / Pii Felicis Aug(usti) 3] |
| `[D(is)` | EDCS-00000050-0 | whole thing is editorial reconstruction | [D(is) M(anibus)] |
| `M(anibus)]` | EDCS-00000050-0 | whole thing is editorial reconstruction | [D(is) M(anibus)] |
| `[Q(uintus)]` | EDCS-00000068-0 | whole thing is editorial reconstruction | [Q(uintus)] Fabius Q(uinti) f(ilius) / Dionysius / v(otum) s(olvit) l(ibens) a(nimo) |
| `[h(ic)]` | EDCS-00000071-0 | whole thing is editorial reconstruction | ] / Afr(anius?) / [h(ic)] s(itus?) [e(st)] |
| `[e(st)]` | EDCS-00000071-0 | whole thing is editorial reconstruction | ] / Afr(anius?) / [h(ic)] s(itus?) [e(st)] |
| `[h(ic)]` | EDCS-00000073-0 | whole thing is editorial reconstruction | ] / [3 pi]issi[m 3] / [h(ic)] s(it?) e(st) |

**What recovery would gain.**

| measure | value | share |
| --- | --- | --- |
| pairs recoverable in principle | 253,256 |  |
| with a usable abbreviation and expansion | 219,509 | 86.67% |
| distinct abbreviation forms | 18,009 |  |
| of those, forms absent from the kept set | 10,708 | 59.46% |
| distinct (abbrev, expansion) types | 26,842 |  |
| of those, types absent from the kept set | 15,021 | 55.96% |

Duplication cuts two ways. By **type**, 55.96% of the pair types here are new to the dataset (15,021 of 26,842) — a real gain in coverage of rare forms. By **token**, 89.98% of the individual pairs repeat a type the kept set already holds, because the volume sits in the same handful of funerary and imperial formulae. Recovering this category would therefore add a long tail of genuinely new forms while re-weighting the head that is already over-represented.

**What recovery would cost.** The dominant sub-class is total reconstruction: the editor inferred the missing letters *and* the expansion of the abbreviation those letters spell. Both halves of the label come from the same act of scholarly inference, so a model trained on them learns the editor's restoration habits and is then evaluated on those same habits. That is circular, and it is worst precisely where the formulae are most predictable — which is why the duplication figure above is so high.

The sub-class *abbreviation partly restored, expansion on surviving text* is different in kind. There the parenthesis expands letters that are actually on the stone; only the earlier part of the word is supplied. `frumen]t(o)` is a real reading of a real abbreviation with a damaged left edge.

**Bias check.** How the dropped pairs compare with the kept pairs.

| measure | value | reading |
| --- | --- | --- |
| province distribution (TVD) | 0.141 | mild skew |
| century distribution (TVD) | 0.126 | mild skew |
| median inscription length | 174 vs 119 kept | dropped pairs come from longer, more damaged texts |
| mean inscription length | 596 vs 280 kept |  |

Total variation distance: 0.00 means the dropped pairs are spread exactly like the kept ones, 1.00 means they share no common ground. Anything above about 0.15 is a materially different population.

Provinces, dropped share against kept share:

| province | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| Roma | 49,420 | 19.51% | 22.62% | 0.86 |
| Africa proconsularis | 19,879 | 7.85% | 8.57% | 0.92 |
| Latium et Campania / Regio I | 19,237 | 7.60% | 8.79% | 0.86 |
| Provincia incerta | 16,275 | 6.43% | 1.85% | 3.47 |
| Numidia | 11,452 | 4.52% | 7.72% | 0.59 |
| Hispania citerior | 8,563 | 3.38% | 3.18% | 1.06 |
| Germania superior | 7,322 | 2.89% | 2.68% | 1.08 |
| Dacia | 6,890 | 2.72% | 1.64% | 1.66 |

Centuries, dropped share against kept share (centuries below 0.5% of this category's drops omitted as noise):

| century | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| 2BC | 1,866 | 0.74% | 0.26% | 2.83 |
| 1BC | 5,953 | 2.35% | 3.00% | 0.78 |
| 1AD | 26,652 | 10.52% | 13.33% | 0.79 |
| 2AD | 59,100 | 23.34% | 19.84% | 1.18 |
| 3AD | 37,928 | 14.98% | 9.96% | 1.50 |
| 4AD | 16,128 | 6.37% | 3.80% | 1.68 |
| 5AD | 4,386 | 1.73% | 0.87% | 2.00 |
| 6AD | 1,685 | 0.67% | 0.51% | 1.30 |

Abbreviation frequency profile of the dropped pairs, measured against how often each form survives in the kept set:

| profile | dropped pairs | share |
| --- | --- | --- |
| form never seen in the kept set | 16,396 | 7.47% |
| form seen fewer than 10 times | 7,745 | 3.53% |
| form seen 10+ times (already well covered) | 195,368 | 89.00% |

**Recommendation: NEEDS HUMAN REVIEW.** Not because the counting is uncertain — it is not — but because the decision turns on a question this script cannot answer: what counts as the input side of the task.

If the input is *the text as printed in EDCS*, then 64,991 partly-restored pairs are perfectly valid — `tribuni]c(ia)` gives the mapping *tribunic -> tribunicia*, which is sound Latin however the letters arrived on the page. If the input is *what the stone actually carries*, they are not, because most of the abbreviation is the editor's supplement. That is a project-defining choice about whether the dataset models epigraphic reading or editorial convention, and a Latinist should make it rather than a heuristic. The 149,582 fully-reconstructed pairs should stay out under either reading.

## 2. `editorial_marker_paren` — 58,720

**Sub-classification.** One label was hiding several situations.

| sub-class | count | share of category |
| --- | --- | --- |
| abbreviation present, editor could not resolve it | 42,805 | 72.90% |
| word marked sic, not an abbreviation | 9,064 | 15.44% |
| abbreviation present, reading marked uncertain | 3,608 | 6.14% |
| standalone (!) sic mark | 2,285 | 3.89% |
| standalone (?) uncertainty mark | 574 | 0.98% |
| bare empty parentheses, no abbreviation | 384 | 0.65% |

**40 examples.**

| raw token | inscription id | sub-class | inscription_text |
| --- | --- | --- | --- |
| `PR()` | EDCS-00000011-0 | abbreviation present, editor could not resolve it | [Ho]muncioni(s) / PR() P() |
| `P()` | EDCS-00000011-0 | abbreviation present, editor could not resolve it | [Ho]muncioni(s) / PR() P() |
| `uac(?)` | EDCS-00000027-0 | abbreviation present, reading marked uncertain | Sabin(a)e bene merenti / coniugi dulcissim(a)e / qu(a)e vixit ann<o=U>s XXVII m(enses) VIII / qu(a)e vixit cu<m=N> marit<o=U> annis VII m(ensibus) IIII / uac(?) sept<e=I>m{u}decim{u} kal(endas) Septembres |
| `M()` | EDCS-00000043-0 | abbreviation present, editor could not resolve it | M() Ae() Bo() |
| `Ae()` | EDCS-00000043-0 | abbreviation present, editor could not resolve it | M() Ae() Bo() |
| `Bo()` | EDCS-00000043-0 | abbreviation present, editor could not resolve it | M() Ae() Bo() |
| `Cass()` | EDCS-00000044-0 | abbreviation present, editor could not resolve it | Cass() P() P() |
| `P()` | EDCS-00000044-0 | abbreviation present, editor could not resolve it | Cass() P() P() |
| `P()` | EDCS-00000044-0 | abbreviation present, editor could not resolve it | Cass() P() P() |
| `I()` | EDCS-00000045-0 | abbreviation present, editor could not resolve it | I() [centuria] Vi() |
| `Vi()` | EDCS-00000045-0 | abbreviation present, editor could not resolve it | I() [centuria] Vi() |
| `Flo()` | EDCS-00000052-0 | abbreviation present, editor could not resolve it | M(arci) Aebuti Flo() |
| `Cel()` | EDCS-00000053-0 | abbreviation present, editor could not resolve it | M(arci) Ati(li?) Cel() |
| `Cel()` | EDCS-00000055-0 | abbreviation present, editor could not resolve it | P(ubli) Cor(neli?) Cel() |
| `L()` | EDCS-00000064-0 | abbreviation present, editor could not resolve it | P(ubli) L() A() |
| `A()` | EDCS-00000064-0 | abbreviation present, editor could not resolve it | P(ubli) L() A() |
| `Q()` | EDCS-00000070-0 | abbreviation present, editor could not resolve it | ] / Q() M[3] / Avi p(onendum) [3] |
| `(?)` | EDCS-00000077-0 | standalone (?) uncertainty mark | D(is) M(anibus) / Q(uinti) Quirini Ruf(i) / DEB (?) ROM (?) / AETER Imper(atoris) / M(arci) Aurel(i) / miles / ann(is) XXXXII / vix(it) / Ant(onius) h(oc) m(onumentum) c(arissimo?) p(osuit) / s(it) t(ibi) t(erra) l(evis) |
| `(?)` | EDCS-00000077-0 | standalone (?) uncertainty mark | D(is) M(anibus) / Q(uinti) Quirini Ruf(i) / DEB (?) ROM (?) / AETER Imper(atoris) / M(arci) Aurel(i) / miles / ann(is) XXXXII / vix(it) / Ant(onius) h(oc) m(onumentum) c(arissimo?) p(osuit) / s(it) t(ibi) t(erra) l(evis) |
| `M()` | EDCS-00000103-0 | abbreviation present, editor could not resolve it | [3]io M() f() Ga[l(eria) 3] / [3 II]viro / [ |
| `f()` | EDCS-00000103-0 | abbreviation present, editor could not resolve it | [3]io M() f() Ga[l(eria) 3] / [3 II]viro / [ |
| `P()` | EDCS-00000104-0 | abbreviation present, editor could not resolve it | P() A[3] P() f() G[al(eria)? 3] / [3]V[3] |
| `P()` | EDCS-00000104-0 | abbreviation present, editor could not resolve it | P() A[3] P() f() G[al(eria)? 3] / [3]V[3] |
| `f()` | EDCS-00000104-0 | abbreviation present, editor could not resolve it | P() A[3] P() f() G[al(eria)? 3] / [3]V[3] |
| `S()` | EDCS-00000129-0 | abbreviation present, editor could not resolve it | S() P() R() |
| `P()` | EDCS-00000129-0 | abbreviation present, editor could not resolve it | S() P() R() |
| `R()` | EDCS-00000129-0 | abbreviation present, editor could not resolve it | S() P() R() |
| `Sex()` | EDCS-00000130-0 | abbreviation present, editor could not resolve it | Sex() Vibi / Crescentis |
| `p()` | EDCS-00000133-0 | abbreviation present, editor could not resolve it | ] / [3]o p() I / [3]o p() I / [3]to p() I / [A]mbillo p() I / [C]resenti VIIII / Modesto I / Neutoni+[3] III |
| `p()` | EDCS-00000133-0 | abbreviation present, editor could not resolve it | ] / [3]o p() I / [3]o p() I / [3]to p() I / [A]mbillo p() I / [C]resenti VIIII / Modesto I / Neutoni+[3] III |
| `p()` | EDCS-00000133-0 | abbreviation present, editor could not resolve it | ] / [3]o p() I / [3]o p() I / [3]to p() I / [A]mbillo p() I / [C]resenti VIIII / Modesto I / Neutoni+[3] III |
| `p()` | EDCS-00000133-0 | abbreviation present, editor could not resolve it | ] / [3]o p() I / [3]o p() I / [3]to p() I / [A]mbillo p() I / [C]resenti VIIII / Modesto I / Neutoni+[3] III |
| `C()` | EDCS-00000146-0 | abbreviation present, editor could not resolve it | C() M() Vin() |
| `M()` | EDCS-00000146-0 | abbreviation present, editor could not resolve it | C() M() Vin() |
| `Vin()` | EDCS-00000146-0 | abbreviation present, editor could not resolve it | C() M() Vin() |
| `C()` | EDCS-00000147-0 | abbreviation present, editor could not resolve it | C() S() C() |
| `S()` | EDCS-00000147-0 | abbreviation present, editor could not resolve it | C() S() C() |
| `C()` | EDCS-00000147-0 | abbreviation present, editor could not resolve it | C() S() C() |
| `Tur()` | EDCS-00000179-0 | abbreviation present, editor could not resolve it | [3] invicto / [co]nservatori / [I]mp(eratoris) senatus p(opuli)q(ue) R(omani) / leg(ionibus) VII Cl(audia) et IIII Fl(avia) / concordibus / P(ublius) Tur() Iulianus / et P(ublius) Tur() Proculus / et P(ublius) Sossius Antiochu[s / 3]QV[3] T(itus) F(la)v(ius) Maxiumu[s / e]t Ap(pius ?) Flavius B[3 /  […] |
| `Tur()` | EDCS-00000179-0 | abbreviation present, editor could not resolve it | [3] invicto / [co]nservatori / [I]mp(eratoris) senatus p(opuli)q(ue) R(omani) / leg(ionibus) VII Cl(audia) et IIII Fl(avia) / concordibus / P(ublius) Tur() Iulianus / et P(ublius) Tur() Proculus / et P(ublius) Sossius Antiochu[s / 3]QV[3] T(itus) F(la)v(ius) Maxiumu[s / e]t Ap(pius ?) Flavius B[3 /  […] |

**Reasoned answer to the question posed.** Yes, and this is the most interesting thing in the audit.

`PR()` and `M()` are not noise. An empty parenthesis is the editor recording that an abbreviation is present on the stone and cannot be resolved. There are **42,805** such tokens, each one a genuine abbreviation with a known surface form and a deliberately withheld expansion.

That is exactly the supervision an abstention class needs, and it cannot be manufactured. Sampling random abbreviations and hiding their answers produces cases that are unresolvable-by-construction; these are cases that a professional epigrapher, holding the stone and the whole formulaic context, judged unresolvable. A model that can predict *this one cannot be expanded* is more useful than one that always guesses, and it can only learn that distinction from labels like these.

The rest of the category is different and should stay out. The `(!)` sub-classes (9,064 tokens) are *sic* marks: the editor is flagging a spelling error on the stone, not expanding an abbreviation. Standalone `(?)` marks carry no abbreviation at all.

**Bias check.** How the dropped pairs compare with the kept pairs.

| measure | value | reading |
| --- | --- | --- |
| province distribution (TVD) | 0.197 | materially different |
| century distribution (TVD) | 0.269 | materially different |
| median inscription length | 24 vs 119 kept | dropped pairs come from shorter texts |
| mean inscription length | 115 vs 280 kept |  |

Total variation distance: 0.00 means the dropped pairs are spread exactly like the kept ones, 1.00 means they share no common ground. Anything above about 0.15 is a materially different population.

Provinces, dropped share against kept share:

| province | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| Roma | 11,606 | 19.76% | 22.62% | 0.87 |
| Latium et Campania / Regio I | 5,531 | 9.42% | 8.79% | 1.07 |
| Africa proconsularis | 3,156 | 5.37% | 8.57% | 0.63 |
| Venetia et Histria / Regio X | 3,145 | 5.36% | 3.42% | 1.56 |
| Gallia Narbonensis | 3,041 | 5.18% | 2.49% | 2.08 |
| Hispania citerior | 2,610 | 4.44% | 3.18% | 1.40 |
| Germania superior | 2,361 | 4.02% | 2.68% | 1.50 |
| Britannia | 1,898 | 3.23% | 1.90% | 1.70 |

Centuries, dropped share against kept share (centuries below 0.5% of this category's drops omitted as noise):

| century | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| 1BC | 840 | 1.43% | 3.00% | 0.48 |
| 1AD | 3,623 | 6.17% | 13.33% | 0.46 |
| 2AD | 4,810 | 8.19% | 19.84% | 0.41 |
| 3AD | 2,453 | 4.18% | 9.96% | 0.42 |
| 4AD | 1,882 | 3.21% | 3.80% | 0.84 |
| 5AD | 568 | 0.97% | 0.87% | 1.12 |

Abbreviation frequency profile of the dropped pairs, measured against how often each form survives in the kept set:

| profile | dropped pairs | share |
| --- | --- | --- |
| form never seen in the kept set | 733 | 57.27% |
| form seen fewer than 10 times | 235 | 18.36% |
| form seen 10+ times (already well covered) | 312 | 24.38% |

## 3. `non_alphabetic_abbrev` — 16,335

**Sub-classification.** One label was hiding several situations.

| sub-class | count | share of category |
| --- | --- | --- |
| symbol abbreviation (\| = the reversed-C and similar signs) | 13,737 | 84.10% |
| other non-letter character in the abbreviation | 2,572 | 15.75% |
| mixed script in the abbreviation | 26 | 0.16% |

**40 examples.**

| raw token | inscription id | sub-class | inscription_text |
| --- | --- | --- | --- |
| `\|(obitus)` | EDCS-00000329-0 | symbol abbreviation (\| = the reversed-C and similar signs) | ] / [3] +N[3]+[3] / \|(obitus) an(norum) XX et Pegissa (?) [3] / Bruti \|(obitus) an(norum) XX f(ilia) GEN+[3] / [n]epoti [3] / [ |
| `\|(obitus)` | EDCS-00000329-0 | symbol abbreviation (\| = the reversed-C and similar signs) | ] / [3] +N[3]+[3] / \|(obitus) an(norum) XX et Pegissa (?) [3] / Bruti \|(obitus) an(norum) XX f(ilia) GEN+[3] / [n]epoti [3] / [ |
| `Aὐρ(ήλιος)` | EDCS-00000529-0 | mixed script in the abbreviation | Aὐρ(ήλιος) Κονωνια/[ν]ὸς Λόγγος νει/[κή]σας παίδων / [πά]λην θέμιδος / [Τυ]διανῆς ἀγω/[ν]οθετοῦντος / [Α]ὐρ(ηλίου) Καλλικλι/[α]νοῦ Ῥουφεινι/[α]νοῦ Ποτείτου |
| `\|(miliaria)` | EDCS-00000620-0 | symbol abbreviation (\| = the reversed-C and similar signs) | Imp(erator) Caes(ar) divi Hadriani f(ilius) divi Traiani / Parthic(i) nep(os) divi Nervae pronep(os) / T(itus) Aelius Hadrianus Antoninus Aug(ustus) Pius / pont(ifex) max(imus) tr(ibunicia) pot(estate) XVII imp(erator) II co(n)s(ul) IIII p(ater) p(atriae) / equitib(us) et peditib(us) qui milit(averu […] |
| `\|(miliaria)` | EDCS-00000620-0 | symbol abbreviation (\| = the reversed-C and similar signs) | Imp(erator) Caes(ar) divi Hadriani f(ilius) divi Traiani / Parthic(i) nep(os) divi Nervae pronep(os) / T(itus) Aelius Hadrianus Antoninus Aug(ustus) Pius / pont(ifex) max(imus) tr(ibunicia) pot(estate) XVII imp(erator) II co(n)s(ul) IIII p(ater) p(atriae) / equitib(us) et peditib(us) qui milit(averu […] |
| `\|(miliaria)` | EDCS-00000639-0 | symbol abbreviation (\| = the reversed-C and similar signs) | Imp(erator) Caes(ar) [divi Hadriani f(ilius) divi Traia]/ni Parth[ic(i) n(epos) divi Nervae pron(epos) T(itus) Ae]/lius Had[rian(us) Antonin(us) Aug(ustus) Pius pont(ifex)] / max(imus) tr[ib(unicia) pot(estate) XVIIII imp(erator) II co(n)s(ul) IV p(ater) p(atriae)] / equitib(us) et [peditib(us) qui  […] |
| `\|(miliaria)` | EDCS-00000639-0 | symbol abbreviation (\| = the reversed-C and similar signs) | Imp(erator) Caes(ar) [divi Hadriani f(ilius) divi Traia]/ni Parth[ic(i) n(epos) divi Nervae pron(epos) T(itus) Ae]/lius Had[rian(us) Antonin(us) Aug(ustus) Pius pont(ifex)] / max(imus) tr[ib(unicia) pot(estate) XVIIII imp(erator) II co(n)s(ul) IV p(ater) p(atriae)] / equitib(us) et [peditib(us) qui  […] |
| `\|(crux)` | EDCS-00000684-0 | symbol abbreviation (\| = the reversed-C and similar signs) | γ´ \|(crux) α´ |
| `\|(denarius)` | EDCS-00000704-0 | symbol abbreviation (\| = the reversed-C and similar signs) | \|(denarius) χῶμα \|(denarius) |
| `\|(denarius)` | EDCS-00000704-0 | symbol abbreviation (\| = the reversed-C and similar signs) | \|(denarius) χῶμα \|(denarius) |
| `\|(deunx)` | EDCS-00000839-1 | symbol abbreviation (\| = the reversed-C and similar signs) | N (librae) VII \|(deunx) // Silius |
| `«Dd(ominis)` | EDCS-00000893-0 | other non-letter character in the abbreviation | «Dd(ominis) nn(ostris)» / «Constantino Maximo» / «[victori ac triumf]ator[i]» / «[semper] Aug(usto) [et Cons]tantin[o]» / «[et Con]stant[i et Constantio nob(ilissimis)]» / «Caess(aribus)» / [6]? |
| `nn(ostris)»` | EDCS-00000893-0 | other non-letter character in the abbreviation | «Dd(ominis) nn(ostris)» / «Constantino Maximo» / «[victori ac triumf]ator[i]» / «[semper] Aug(usto) [et Cons]tantin[o]» / «[et Con]stant[i et Constantio nob(ilissimis)]» / «Caess(aribus)» / [6]? |
| `«Caess(aribus)»` | EDCS-00000893-0 | other non-letter character in the abbreviation | «Dd(ominis) nn(ostris)» / «Constantino Maximo» / «[victori ac triumf]ator[i]» / «[semper] Aug(usto) [et Cons]tantin[o]» / «[et Con]stant[i et Constantio nob(ilissimis)]» / «Caess(aribus)» / [6]? |
| `⟦Fl(avio)` | EDCS-00000894-0 | other non-letter character in the abbreviation | DD(ominis) NN(ostris) Impp(eratoribus) Caes[s(aribus)] / Fl(avio) Constantino p(io) f(elici) / max(imo) victor<i=E> (!) ac trium/fatori semper Aug(usto) / et ⟦Fl(avio) Constanti⟧no / et ⟦Fl(avio) Constan⟧tio / et ⟦[Fl(avio)] Cons⟧tanti nobb[b(ilissimis)] / ⟦[Caesss(aribus)]⟧ |
| `⟦Fl(avio)` | EDCS-00000894-0 | other non-letter character in the abbreviation | DD(ominis) NN(ostris) Impp(eratoribus) Caes[s(aribus)] / Fl(avio) Constantino p(io) f(elici) / max(imo) victor<i=E> (!) ac trium/fatori semper Aug(usto) / et ⟦Fl(avio) Constanti⟧no / et ⟦Fl(avio) Constan⟧tio / et ⟦[Fl(avio)] Cons⟧tanti nobb[b(ilissimis)] / ⟦[Caesss(aribus)]⟧ |
| `«D(omino)` | EDCS-00000894-1 | other non-letter character in the abbreviation | «D(omino) n(ostro) Fl(avio) Iuliano» / «max(imo) ac triumf(atori)» / «semper Aug(usto)» |
| `«max(imo)` | EDCS-00000894-1 | other non-letter character in the abbreviation | «D(omino) n(ostro) Fl(avio) Iuliano» / «max(imo) ac triumf(atori)» / «semper Aug(usto)» |
| `triumf(atori)»` | EDCS-00000894-1 | other non-letter character in the abbreviation | «D(omino) n(ostro) Fl(avio) Iuliano» / «max(imo) ac triumf(atori)» / «semper Aug(usto)» |
| `Aug(usto)»` | EDCS-00000894-1 | other non-letter character in the abbreviation | «D(omino) n(ostro) Fl(avio) Iuliano» / «max(imo) ac triumf(atori)» / «semper Aug(usto)» |
| `«p(io)` | EDCS-00000895-1 | other non-letter character in the abbreviation | «[3] Constantino» / «p(io) f(elici) invicto Au(gusto)» / «⟦3⟧+o» / «⟦3⟧ et» / «⟦6⟧» / «⟦3⟧ et» / «⟦[3 no]⟧bb(ilissimis) Caess(aribus)» |
| `Au(gusto)»` | EDCS-00000895-1 | other non-letter character in the abbreviation | «[3] Constantino» / «p(io) f(elici) invicto Au(gusto)» / «⟦3⟧+o» / «⟦3⟧ et» / «⟦6⟧» / «⟦3⟧ et» / «⟦[3 no]⟧bb(ilissimis) Caess(aribus)» |
| `Caess(aribus)»` | EDCS-00000895-1 | other non-letter character in the abbreviation | «[3] Constantino» / «p(io) f(elici) invicto Au(gusto)» / «⟦3⟧+o» / «⟦3⟧ et» / «⟦6⟧» / «⟦3⟧ et» / «⟦[3 no]⟧bb(ilissimis) Caess(aribus)» |
| `q̅(uaestori)` | EDCS-00000939-0 | other non-letter character in the abbreviation | D(is) M(anibus). / Paternio Sperato, / Augustal(i) Cumis, q̅(uaestori), / qui vixit ann(is) XXXVIII, / mensib(us) V, d(iebus) X, / Sempronia Primilla / soror fratri / benemerenti fecit. |
| `\|(denarii)` | EDCS-00000941-1 | symbol abbreviation (\| = the reversed-C and similar signs) | [3] a mori(no) / ital(ico) / \|(denarii) XXIX |
| `\|(centurionis)` | EDCS-00000943-0 | symbol abbreviation (\| = the reversed-C and similar signs) | T(iti) Flavini Veri \|(centurionis) |
| `m(erito)⟧` | EDCS-00000971-0 | other non-letter character in the abbreviation | [ ]usonius ⟦Eros Iovi v(otum) s(olvit) l(ibens) m(erito)⟧ |
| `\|(mulieris)` | EDCS-00001114-0 | symbol abbreviation (\| = the reversed-C and similar signs) | Kania \|(mulieris) l(iberta) Salvia viva / fecit sibi et / C(aio) Kanio [1 l(iberto)] Philocli / vir[o s]uo. |
| `§An(na)ei` | EDCS-00001341-0 | other non-letter character in the abbreviation | Αν(να)ει Ου/(α)λεντε(ς) / αννωρο(μ) / κοδραγε(ν)/τα οππρε/σ(σ)ειτ ρουει/να §An(na)ei V/(a)lente(s) / annoru(m) / quadragi(n)/ta oppre/s(s)it rui/na |
| `\|(centvria)` | EDCS-00001355-0 | symbol abbreviation (\| = the reversed-C and similar signs) | \|(centvria) Galli / C(ai) Salvi |
| `\|(milliaria)` | EDCS-00001499-0 | symbol abbreviation (\| = the reversed-C and similar signs) | Imp(erator) Caes(ar) divi Hadr[iani f(ilius) divi Traia]/ni Parthic(i) nep(os) div[i Nervae pronep(os)] / T(itus) Aelius Hadrianus [Antoninus Aug(ustus) Pi]/us pont(ifex) max(imus) tr(ibunicia) pot(estate) [X 3 imp(erator) II co(n)s(ul) IV p(ater) p(atriae)] / equitib(us) et pedit(ibus) qui [milit(a […] |
| `\|(mulieris)` | EDCS-00380047-0 | symbol abbreviation (\| = the reversed-C and similar signs) | T(itus) Caesius \|(mulieris) l(ibertus) / Urbanus / T(itus) Caesius \|(mulieris) l(ibertus) Eros / in fr(onte) p(edes) XII in ag(ro) p(edes) XX |
| `\|(mulieris)` | EDCS-00380047-0 | symbol abbreviation (\| = the reversed-C and similar signs) | T(itus) Caesius \|(mulieris) l(ibertus) / Urbanus / T(itus) Caesius \|(mulieris) l(ibertus) Eros / in fr(onte) p(edes) XII in ag(ro) p(edes) XX |
| `\|(mulieris)` | EDCS-00380057-0 | symbol abbreviation (\| = the reversed-C and similar signs) | Vinnia \|(mulieris) l(iberta) [3] / in fronte ped(es) XV [ |
| `\|(centurionis)` | EDCS-00380082-0 | symbol abbreviation (\| = the reversed-C and similar signs) | D(is) M(anibus) / C(ai) Iuli Proculi / \|(centurionis) leg(ionis) XIIII Gem(inae) / a peregrinis / C(aius) Iulius Valerianus / fratri optimo |
| `\|(mulieris)` | EDCS-00380087-0 | symbol abbreviation (\| = the reversed-C and similar signs) | Caesia / \|(mulieris) l(iberta) Phaen/usa h(ic) s(ita) e(st) |
| `\|(mulieris)` | EDCS-00380135-0 | symbol abbreviation (\| = the reversed-C and similar signs) | Cerriniae \|(mulieris) lib(ertae) / Vitali et Cerrini/ae Sex(ti) f(iliae) Proculae / et Sex(to) Cerrinio / Germullo obito / in fr(onte) p(edes) XII in ag(ro) / p(edes) X |
| `\|(mulieris)` | EDCS-00380138-0 | symbol abbreviation (\| = the reversed-C and similar signs) | [Sa]ssiae(?) \|(mulieris) l(ibertae) / Hora[e] / in ag[r(o) p(edes) 3] / in [fr(onte) p(edes) |
| `\|(denarii)` | EDCS-00380167-0 | symbol abbreviation (\| = the reversed-C and similar signs) | D(is) M(anibus) / Gerontio / Felicissimo / Re() Eventiane co(n)i/ugi kari(ssimo) suo c<u=O>/<m=N> qu<o=E> vixit an(nos) XVII / m(enses) VI quem so/lam dereli[n]/quit b(ene) m(erenti) p(osuit) / \|(denarii) CL |
| `\|(mulieris)` | EDCS-00380200-0 | symbol abbreviation (\| = the reversed-C and similar signs) | L(ucius) Gellius L(uci) l(ibertus) It<y=U>s / Serv{e}ilia \|(mulieris) l(iberta) Stadium / P(ublius) Serv{e}ilius \|(mulieris) l(ibertus) Dius |

The bulk is the `|` symbol, which stands for epigraphic signs the transcription cannot render as a letter — most often the reversed C (Ɔ) for *mulieris*, and the centurial sign. `|(mulieris)` is a true abbreviation-expansion pair whose abbreviation happens to be a glyph rather than a letter.

Whether that belongs in the dataset depends on the task definition. If the input is text as printed, the model would have to expand a `|` it can see, which is legitimate and learnable. If the task is strictly letters-to-letters, these are out of scope. They are consistent and machine-readable either way, so this is a scoping decision rather than a data-quality problem.

**What recovery would gain.**

| measure | value | share |
| --- | --- | --- |
| pairs recoverable in principle | 16,335 |  |
| with a usable abbreviation and expansion | 2,720 | 16.65% |
| distinct abbreviation forms | 485 |  |
| of those, forms absent from the kept set | 119 | 24.54% |
| distinct (abbrev, expansion) types | 809 |  |
| of those, types absent from the kept set | 233 | 28.80% |

Duplication cuts two ways. By **type**, 28.80% of the pair types here are new to the dataset (233 of 809) — a real gain in coverage of rare forms. By **token**, 89.56% of the individual pairs repeat a type the kept set already holds, because the volume sits in the same handful of funerary and imperial formulae. Recovering this category would therefore add a long tail of genuinely new forms while re-weighting the head that is already over-represented.

**Bias check.** How the dropped pairs compare with the kept pairs.

| measure | value | reading |
| --- | --- | --- |
| province distribution (TVD) | 0.214 | materially different |
| century distribution (TVD) | 0.109 | mild skew |
| median inscription length | 186 vs 119 kept | dropped pairs come from longer, more damaged texts |
| mean inscription length | 447 vs 280 kept |  |

Total variation distance: 0.00 means the dropped pairs are spread exactly like the kept ones, 1.00 means they share no common ground. Anything above about 0.15 is a materially different population.

Provinces, dropped share against kept share:

| province | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| Roma | 4,305 | 26.35% | 22.62% | 1.17 |
| Latium et Campania / Regio I | 1,692 | 10.36% | 8.79% | 1.18 |
| Asia | 1,050 | 6.43% | 0.52% | 12.26 |
| Britannia | 1,041 | 6.37% | 1.90% | 3.36 |
| Africa proconsularis | 760 | 4.65% | 8.57% | 0.54 |
| Numidia | 634 | 3.88% | 7.72% | 0.50 |
| Germania superior | 516 | 3.16% | 2.68% | 1.18 |
| Gallia Narbonensis | 422 | 2.58% | 2.49% | 1.04 |

Centuries, dropped share against kept share (centuries below 0.5% of this category's drops omitted as noise):

| century | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| 1BC | 843 | 5.16% | 3.00% | 1.72 |
| 1AD | 3,196 | 19.57% | 13.33% | 1.47 |
| 2AD | 2,535 | 15.52% | 19.84% | 0.78 |
| 3AD | 2,021 | 12.37% | 9.96% | 1.24 |
| 4AD | 292 | 1.79% | 3.80% | 0.47 |

Abbreviation frequency profile of the dropped pairs, measured against how often each form survives in the kept set:

| profile | dropped pairs | share |
| --- | --- | --- |
| form never seen in the kept set | 129 | 4.74% |
| form seen fewer than 10 times | 83 | 3.05% |
| form seen 10+ times (already well covered) | 2,508 | 92.21% |

## 4. `greek_script` — 12,987

**Sub-classification.** One label was hiding several situations.

| sub-class | count | share of category |
| --- | --- | --- |
| (not sub-classified) | 12,987 | 100.00% |

**40 examples.**

| raw token | inscription id | sub-class | inscription_text |
| --- | --- | --- | --- |
| `Αύρ(ηλίου)` | EDCS-00000155-0 | (not sub-classified) | Σορός Αύρ(ηλίου) Ούρανίου μαρι[ |
| `δδ(εοποτῶν)` | EDCS-00000250-0 | (not sub-classified) | Θεοῦ πρωνοί[ᾳ] / ὑπὲρ νείκης \| τῶν δδ(εοποτῶν) ἡμῶν / τόνδε τὸν βο/τρυόκομον Σε/μελήιον κισσο/στεφὴν δίγονον / θιασῶν μύστην / Θεόδωρος καὶ / Παλλάδιος οἱ / τῆς ζ´ἰνδ(ικτιῶος) Ἑλ[λη]σπόντου / [ππ]ρρ(ιμιπιλάριοι) θρέψαν/[τες 3] / [3 ᾽Ιτα]λικὴν ἀνε/θήκαμεν. |
| `ζ´ἰνδ(ικτιῶος)` | EDCS-00000250-0 | (not sub-classified) | Θεοῦ πρωνοί[ᾳ] / ὑπὲρ νείκης \| τῶν δδ(εοποτῶν) ἡμῶν / τόνδε τὸν βο/τρυόκομον Σε/μελήιον κισσο/στεφὴν δίγονον / θιασῶν μύστην / Θεόδωρος καὶ / Παλλάδιος οἱ / τῆς ζ´ἰνδ(ικτιῶος) Ἑλ[λη]σπόντου / [ππ]ρρ(ιμιπιλάριοι) θρέψαν/[τες 3] / [3 ᾽Ιτα]λικὴν ἀνε/θήκαμεν. |
| `Κλ(αυδίου)` | EDCS-00000253-0 | (not sub-classified) | Κλ(αυδίου) Κέλερος // Λί(τραι) θ´ οὐ(γκίαι) ιγ´(γράμματα) θ´ |
| `Λί(τραι)` | EDCS-00000253-0 | (not sub-classified) | Κλ(αυδίου) Κέλερος // Λί(τραι) θ´ οὐ(γκίαι) ιγ´(γράμματα) θ´ |
| `οὐ(γκίαι)` | EDCS-00000253-0 | (not sub-classified) | Κλ(αυδίου) Κέλερος // Λί(τραι) θ´ οὐ(γκίαι) ιγ´(γράμματα) θ´ |
| `ιγ´(γράμματα)` | EDCS-00000253-0 | (not sub-classified) | Κλ(αυδίου) Κέλερος // Λί(τραι) θ´ οὐ(γκίαι) ιγ´(γράμματα) θ´ |
| `Σεβ(αστοῦ)` | EDCS-00000258-0 | (not sub-classified) | Ἀγαθῇ Τύχῃ / ἔτους βπσ´ Σεβ(αστοῦ) / τοῦ καὶ ηοτ´Δαισί/ου ηἰ ἐν Πύδνῃ / οἱ συνελθόντες / θρησκευταὶ ἐπὶ θεοῦ / Δ[ιὸς] ῾Υψίστου ἔθεν/το τήνδε τὴν στήλην / λογιστεύοντος Οὐρ/βανιανοῦ Βιλίστου / ἄρχοντος Αὐρ(ηλἰου) Νιγερ[ί]/ωνος ὐπὸ ἀρχισυνά/γωγον Αὐρ(ήλιον) Κηπίωνα τὸν / πρὶν Πιερίωνος καὶ προστἀτου / […] |
| `Αὐρ(ηλἰου)` | EDCS-00000258-0 | (not sub-classified) | Ἀγαθῇ Τύχῃ / ἔτους βπσ´ Σεβ(αστοῦ) / τοῦ καὶ ηοτ´Δαισί/ου ηἰ ἐν Πύδνῃ / οἱ συνελθόντες / θρησκευταὶ ἐπὶ θεοῦ / Δ[ιὸς] ῾Υψίστου ἔθεν/το τήνδε τὴν στήλην / λογιστεύοντος Οὐρ/βανιανοῦ Βιλίστου / ἄρχοντος Αὐρ(ηλἰου) Νιγερ[ί]/ωνος ὐπὸ ἀρχισυνά/γωγον Αὐρ(ήλιον) Κηπίωνα τὸν / πρὶν Πιερίωνος καὶ προστἀτου / […] |
| `Αὐρ(ήλιον)` | EDCS-00000258-0 | (not sub-classified) | Ἀγαθῇ Τύχῃ / ἔτους βπσ´ Σεβ(αστοῦ) / τοῦ καὶ ηοτ´Δαισί/ου ηἰ ἐν Πύδνῃ / οἱ συνελθόντες / θρησκευταὶ ἐπὶ θεοῦ / Δ[ιὸς] ῾Υψίστου ἔθεν/το τήνδε τὴν στήλην / λογιστεύοντος Οὐρ/βανιανοῦ Βιλίστου / ἄρχοντος Αὐρ(ηλἰου) Νιγερ[ί]/ωνος ὐπὸ ἀρχισυνά/γωγον Αὐρ(ήλιον) Κηπίωνα τὸν / πρὶν Πιερίωνος καὶ προστἀτου / […] |
| `Αὐρ(ηλίου)` | EDCS-00000258-0 | (not sub-classified) | Ἀγαθῇ Τύχῃ / ἔτους βπσ´ Σεβ(αστοῦ) / τοῦ καὶ ηοτ´Δαισί/ου ηἰ ἐν Πύδνῃ / οἱ συνελθόντες / θρησκευταὶ ἐπὶ θεοῦ / Δ[ιὸς] ῾Υψίστου ἔθεν/το τήνδε τὴν στήλην / λογιστεύοντος Οὐρ/βανιανοῦ Βιλίστου / ἄρχοντος Αὐρ(ηλἰου) Νιγερ[ί]/ωνος ὐπὸ ἀρχισυνά/γωγον Αὐρ(ήλιον) Κηπίωνα τὸν / πρὶν Πιερίωνος καὶ προστἀτου / […] |
| `Αὐρ(ηλίου)` | EDCS-00000258-0 | (not sub-classified) | Ἀγαθῇ Τύχῃ / ἔτους βπσ´ Σεβ(αστοῦ) / τοῦ καὶ ηοτ´Δαισί/ου ηἰ ἐν Πύδνῃ / οἱ συνελθόντες / θρησκευταὶ ἐπὶ θεοῦ / Δ[ιὸς] ῾Υψίστου ἔθεν/το τήνδε τὴν στήλην / λογιστεύοντος Οὐρ/βανιανοῦ Βιλίστου / ἄρχοντος Αὐρ(ηλἰου) Νιγερ[ί]/ωνος ὐπὸ ἀρχισυνά/γωγον Αὐρ(ήλιον) Κηπίωνα τὸν / πρὶν Πιερίωνος καὶ προστἀτου / […] |
| `Τ(ίτος)` | EDCS-00000258-0 | (not sub-classified) | Ἀγαθῇ Τύχῃ / ἔτους βπσ´ Σεβ(αστοῦ) / τοῦ καὶ ηοτ´Δαισί/ου ηἰ ἐν Πύδνῃ / οἱ συνελθόντες / θρησκευταὶ ἐπὶ θεοῦ / Δ[ιὸς] ῾Υψίστου ἔθεν/το τήνδε τὴν στήλην / λογιστεύοντος Οὐρ/βανιανοῦ Βιλίστου / ἄρχοντος Αὐρ(ηλἰου) Νιγερ[ί]/ωνος ὐπὸ ἀρχισυνά/γωγον Αὐρ(ήλιον) Κηπίωνα τὸν / πρὶν Πιερίωνος καὶ προστἀτου / […] |
| `Κλαύ(διος)` | EDCS-00000258-0 | (not sub-classified) | Ἀγαθῇ Τύχῃ / ἔτους βπσ´ Σεβ(αστοῦ) / τοῦ καὶ ηοτ´Δαισί/ου ηἰ ἐν Πύδνῃ / οἱ συνελθόντες / θρησκευταὶ ἐπὶ θεοῦ / Δ[ιὸς] ῾Υψίστου ἔθεν/το τήνδε τὴν στήλην / λογιστεύοντος Οὐρ/βανιανοῦ Βιλίστου / ἄρχοντος Αὐρ(ηλἰου) Νιγερ[ί]/ωνος ὐπὸ ἀρχισυνά/γωγον Αὐρ(ήλιον) Κηπίωνα τὸν / πρὶν Πιερίωνος καὶ προστἀτου / […] |
| `Τ(ίτος)` | EDCS-00000258-0 | (not sub-classified) | Ἀγαθῇ Τύχῃ / ἔτους βπσ´ Σεβ(αστοῦ) / τοῦ καὶ ηοτ´Δαισί/ου ηἰ ἐν Πύδνῃ / οἱ συνελθόντες / θρησκευταὶ ἐπὶ θεοῦ / Δ[ιὸς] ῾Υψίστου ἔθεν/το τήνδε τὴν στήλην / λογιστεύοντος Οὐρ/βανιανοῦ Βιλίστου / ἄρχοντος Αὐρ(ηλἰου) Νιγερ[ί]/ωνος ὐπὸ ἀρχισυνά/γωγον Αὐρ(ήλιον) Κηπίωνα τὸν / πρὶν Πιερίωνος καὶ προστἀτου / […] |
| `Τ(ίτος)` | EDCS-00000258-0 | (not sub-classified) | Ἀγαθῇ Τύχῃ / ἔτους βπσ´ Σεβ(αστοῦ) / τοῦ καὶ ηοτ´Δαισί/ου ηἰ ἐν Πύδνῃ / οἱ συνελθόντες / θρησκευταὶ ἐπὶ θεοῦ / Δ[ιὸς] ῾Υψίστου ἔθεν/το τήνδε τὴν στήλην / λογιστεύοντος Οὐρ/βανιανοῦ Βιλίστου / ἄρχοντος Αὐρ(ηλἰου) Νιγερ[ί]/ωνος ὐπὸ ἀρχισυνά/γωγον Αὐρ(ήλιον) Κηπίωνα τὸν / πρὶν Πιερίωνος καὶ προστἀτου / […] |
| `Κορνιφ(ίκιος)` | EDCS-00000258-0 | (not sub-classified) | Ἀγαθῇ Τύχῃ / ἔτους βπσ´ Σεβ(αστοῦ) / τοῦ καὶ ηοτ´Δαισί/ου ηἰ ἐν Πύδνῃ / οἱ συνελθόντες / θρησκευταὶ ἐπὶ θεοῦ / Δ[ιὸς] ῾Υψίστου ἔθεν/το τήνδε τὴν στήλην / λογιστεύοντος Οὐρ/βανιανοῦ Βιλίστου / ἄρχοντος Αὐρ(ηλἰου) Νιγερ[ί]/ωνος ὐπὸ ἀρχισυνά/γωγον Αὐρ(ήλιον) Κηπίωνα τὸν / πρὶν Πιερίωνος καὶ προστἀτου / […] |
| `Τ(ίτον)` | EDCS-00000260-0 | (not sub-classified) | Ἡ π[όλις] Τ(ίτον) Φλά[βιον |
| `λ(ούκιον)` | EDCS-00000261-0 | (not sub-classified) | Δασσα[ρητίων ἄρ]/χοντε[ς βουλή δῆ]/μος λ(ούκιον) Σ[3] Κουίντ[ου 3]/λιον ΣΤ[ |
| `ὁσιώτ(ατος)` | EDCS-00000265-0 | (not sub-classified) | ]α ἀληθῶς φιλόστοργος πατήρ [2]ΠΑΙΟ[3]ΗΜ[3] / ὁ ὁσιώτ(ατος) ἐπίσκ(οπος) κ(αὶ) τοῦτο τὸ ἔργον ἐναρξάμενος καὶ τελε[ιώσας 3]ΟΥΑΥ[3] / κατέλιπεν τ[ο]ῖς ἑαυτοῦ πεσίν ἐξυπηρετοῦντι αὐτῷ Α[1]Υ[ |
| `ἐπίσκ(οπος)` | EDCS-00000265-0 | (not sub-classified) | ]α ἀληθῶς φιλόστοργος πατήρ [2]ΠΑΙΟ[3]ΗΜ[3] / ὁ ὁσιώτ(ατος) ἐπίσκ(οπος) κ(αὶ) τοῦτο τὸ ἔργον ἐναρξάμενος καὶ τελε[ιώσας 3]ΟΥΑΥ[3] / κατέλιπεν τ[ο]ῖς ἑαυτοῦ πεσίν ἐξυπηρετοῦντι αὐτῷ Α[1]Υ[ |
| `κ(αὶ)` | EDCS-00000265-0 | (not sub-classified) | ]α ἀληθῶς φιλόστοργος πατήρ [2]ΠΑΙΟ[3]ΗΜ[3] / ὁ ὁσιώτ(ατος) ἐπίσκ(οπος) κ(αὶ) τοῦτο τὸ ἔργον ἐναρξάμενος καὶ τελε[ιώσας 3]ΟΥΑΥ[3] / κατέλιπεν τ[ο]ῖς ἑαυτοῦ πεσίν ἐξυπηρετοῦντι αὐτῷ Α[1]Υ[ |
| `βραχυτέρο(ις)` | EDCS-00000266-0 | (not sub-classified) | Ὁ βίος βραχύς έσ/τιν ὦ ξένε τὸ / δὲ παιδίον Βιτι/ανὸς εὗρε τοῦτο / ὧδε βραχυτέρο(ις) / χρόνοισιν ἐννέα / καταλελοιπὼς / τὸν βίον ὃς κ(αὶ) γέρο<ν>/τας εἰς τὸ φρονεῖν / ὑπερέβαλεν Βασι/λοῦς γὰρ ἦν βλάστη/μα τῆς ἰλλ(ουστρίας) ὃς κ(αὶ) γονεῦ/σιν οὐ μόνον ἔδωκε / στενεῖν ἀλλὰ πολὺ / πᾶσι κ(αὶ) φἰλοις καὶ […] |
| `κ(αὶ)` | EDCS-00000266-0 | (not sub-classified) | Ὁ βίος βραχύς έσ/τιν ὦ ξένε τὸ / δὲ παιδίον Βιτι/ανὸς εὗρε τοῦτο / ὧδε βραχυτέρο(ις) / χρόνοισιν ἐννέα / καταλελοιπὼς / τὸν βίον ὃς κ(αὶ) γέρο<ν>/τας εἰς τὸ φρονεῖν / ὑπερέβαλεν Βασι/λοῦς γὰρ ἦν βλάστη/μα τῆς ἰλλ(ουστρίας) ὃς κ(αὶ) γονεῦ/σιν οὐ μόνον ἔδωκε / στενεῖν ἀλλὰ πολὺ / πᾶσι κ(αὶ) φἰλοις καὶ […] |
| `ἰλλ(ουστρίας)` | EDCS-00000266-0 | (not sub-classified) | Ὁ βίος βραχύς έσ/τιν ὦ ξένε τὸ / δὲ παιδίον Βιτι/ανὸς εὗρε τοῦτο / ὧδε βραχυτέρο(ις) / χρόνοισιν ἐννέα / καταλελοιπὼς / τὸν βίον ὃς κ(αὶ) γέρο<ν>/τας εἰς τὸ φρονεῖν / ὑπερέβαλεν Βασι/λοῦς γὰρ ἦν βλάστη/μα τῆς ἰλλ(ουστρίας) ὃς κ(αὶ) γονεῦ/σιν οὐ μόνον ἔδωκε / στενεῖν ἀλλὰ πολὺ / πᾶσι κ(αὶ) φἰλοις καὶ […] |
| `κ(αὶ)` | EDCS-00000266-0 | (not sub-classified) | Ὁ βίος βραχύς έσ/τιν ὦ ξένε τὸ / δὲ παιδίον Βιτι/ανὸς εὗρε τοῦτο / ὧδε βραχυτέρο(ις) / χρόνοισιν ἐννέα / καταλελοιπὼς / τὸν βίον ὃς κ(αὶ) γέρο<ν>/τας εἰς τὸ φρονεῖν / ὑπερέβαλεν Βασι/λοῦς γὰρ ἦν βλάστη/μα τῆς ἰλλ(ουστρίας) ὃς κ(αὶ) γονεῦ/σιν οὐ μόνον ἔδωκε / στενεῖν ἀλλὰ πολὺ / πᾶσι κ(αὶ) φἰλοις καὶ […] |
| `κ(αὶ)` | EDCS-00000266-0 | (not sub-classified) | Ὁ βίος βραχύς έσ/τιν ὦ ξένε τὸ / δὲ παιδίον Βιτι/ανὸς εὗρε τοῦτο / ὧδε βραχυτέρο(ις) / χρόνοισιν ἐννέα / καταλελοιπὼς / τὸν βίον ὃς κ(αὶ) γέρο<ν>/τας εἰς τὸ φρονεῖν / ὑπερέβαλεν Βασι/λοῦς γὰρ ἦν βλάστη/μα τῆς ἰλλ(ουστρίας) ὃς κ(αὶ) γονεῦ/σιν οὐ μόνον ἔδωκε / στενεῖν ἀλλὰ πολὺ / πᾶσι κ(αὶ) φἰλοις καὶ […] |
| `ἁγιω(τάτῃ)` | EDCS-00000267-0 | (not sub-classified) | Ἐνθαδε κῖται Ἀνθέμ[ιος] / δουλεύσας τῇ ἁγιω(τάτῃ) / ἔτη ἑξήκοντα δύο / τοῦ βίου ἐδέξατο ἰν[δ(ικτιῶνος) 3] μη(νὶ) Δεκεμβρ(ίου) ε´ ἡμ(έρᾳ) Σα[β(βάτῳ)] |
| `μη(νὶ)` | EDCS-00000267-0 | (not sub-classified) | Ἐνθαδε κῖται Ἀνθέμ[ιος] / δουλεύσας τῇ ἁγιω(τάτῃ) / ἔτη ἑξήκοντα δύο / τοῦ βίου ἐδέξατο ἰν[δ(ικτιῶνος) 3] μη(νὶ) Δεκεμβρ(ίου) ε´ ἡμ(έρᾳ) Σα[β(βάτῳ)] |
| `Δεκεμβρ(ίου)` | EDCS-00000267-0 | (not sub-classified) | Ἐνθαδε κῖται Ἀνθέμ[ιος] / δουλεύσας τῇ ἁγιω(τάτῃ) / ἔτη ἑξήκοντα δύο / τοῦ βίου ἐδέξατο ἰν[δ(ικτιῶνος) 3] μη(νὶ) Δεκεμβρ(ίου) ε´ ἡμ(έρᾳ) Σα[β(βάτῳ)] |
| `ἡμ(έρᾳ)` | EDCS-00000267-0 | (not sub-classified) | Ἐνθαδε κῖται Ἀνθέμ[ιος] / δουλεύσας τῇ ἁγιω(τάτῃ) / ἔτη ἑξήκοντα δύο / τοῦ βίου ἐδέξατο ἰν[δ(ικτιῶνος) 3] μη(νὶ) Δεκεμβρ(ίου) ε´ ἡμ(έρᾳ) Σα[β(βάτῳ)] |
| `κ(αὶ)` | EDCS-00000268-0 | (not sub-classified) | Τύμβος μὲν ἔλαχεν οὗτος τοῦ Καλλικράτους ὦ ξέν[ε] / ὃς ἀοίδημον μνήμην κατέλιπεν γενέτῃ τε / κ(αὶ) τέκνοις [3]ΑΙ φίλοισιν λ´κ(αὶ) ε´ἔτος ἐναθρήσας / τὸ πᾶσιν ἀπ[αραίτητον] τέλος ἐ[δέξ]ατο μη(νὶ) Ἰουνίου δ´ἰνδικτ(ιῶνος) ς |
| `λ´κ(αὶ)` | EDCS-00000268-0 | (not sub-classified) | Τύμβος μὲν ἔλαχεν οὗτος τοῦ Καλλικράτους ὦ ξέν[ε] / ὃς ἀοίδημον μνήμην κατέλιπεν γενέτῃ τε / κ(αὶ) τέκνοις [3]ΑΙ φίλοισιν λ´κ(αὶ) ε´ἔτος ἐναθρήσας / τὸ πᾶσιν ἀπ[αραίτητον] τέλος ἐ[δέξ]ατο μη(νὶ) Ἰουνίου δ´ἰνδικτ(ιῶνος) ς |
| `μη(νὶ)` | EDCS-00000268-0 | (not sub-classified) | Τύμβος μὲν ἔλαχεν οὗτος τοῦ Καλλικράτους ὦ ξέν[ε] / ὃς ἀοίδημον μνήμην κατέλιπεν γενέτῃ τε / κ(αὶ) τέκνοις [3]ΑΙ φίλοισιν λ´κ(αὶ) ε´ἔτος ἐναθρήσας / τὸ πᾶσιν ἀπ[αραίτητον] τέλος ἐ[δέξ]ατο μη(νὶ) Ἰουνίου δ´ἰνδικτ(ιῶνος) ς |
| `δ´ἰνδικτ(ιῶνος)` | EDCS-00000268-0 | (not sub-classified) | Τύμβος μὲν ἔλαχεν οὗτος τοῦ Καλλικράτους ὦ ξέν[ε] / ὃς ἀοίδημον μνήμην κατέλιπεν γενέτῃ τε / κ(αὶ) τέκνοις [3]ΑΙ φίλοισιν λ´κ(αὶ) ε´ἔτος ἐναθρήσας / τὸ πᾶσιν ἀπ[αραίτητον] τέλος ἐ[δέξ]ατο μη(νὶ) Ἰουνίου δ´ἰνδικτ(ιῶνος) ς |
| `Φλ(άβιος)` | EDCS-00000269-0 | (not sub-classified) | Κατὰ τὸ δόξαν / τῷ κρατίστῳ / συνεδρίῳ τῶν / Μακεδόνων / καὶ τῇ Δερριό/πων βουλῇ καὶ / τῷ δήμῳ /Ἰνστέιον / Ἀλέξανδρον / Φλ(άβιος) Παράμονος / καὶ Ἰνστεία / Νικόπολις οἱ / γονεῖς ἀρετῆς/ ἔνεκεν |
| `Τι(βερίῳ)` | EDCS-00000271-0 | (not sub-classified) | Τι(βερίῳ) Καίσαρι θεῷ θεοῦ υἱῶι / Σεβαστῷ καὶ τῇ πόλει Νεικόστρατος καὶ Δημέ/ας Ἀσκληπᾶ καὶ Ἡρακλεόδωρος / Χρησίμου ἀγορανομοῦντες / τὸ ἀγορανόμιον καὶ τὸ προσὸν ἐργαστήριον καὶ σταθμὰ καὶ ζυ/γὰ καὶ μέτρα καὶ τὸν Ἑρμῆ ἐκ τῶ[ν] ἰδίων ἔτους ξ´ Σεβαστ[οῦ] |
| `Τι(βερίου)` | EDCS-00000272-0 | (not sub-classified) | Ἡ πόλις / ἀγονοθετοῦντος Τι(βερίου) Κλαυδὶου / Βακχίου πολιταρχούντοων Μενάν/δρου τοῦ Ἁρπάλου Πὐρρπυ τοῦ Περιγέ/νου Περιγένου τοῦ Νεικοστράτου Πει/ερίωνος τοῦ Διονυσίου, Διονυσίου τοῦ Διο/νυσίου ταμιεύοντος Ζωίλου τοῦ Ζωιλου / τοῦ καὶ Λουκίου |
| `Δ(έκμον)` | EDCS-00000273-0 | (not sub-classified) | Ἠ πόλις / Δ(έκμον) Τερέντιον Δέκμου υἱὸν Κυρείνα / Γεντιανὸν Γνάιον Μινίκιον Φαυστεῖνον / τεσσάρων / ἀνδρῶν ὁδῶν ἐπιμελητήν χει/λίαρχον λεγιώνον Ἐνδεκάτης Κλαυδίας Εὐσε/βοῦς Πιστῆς καὶ Ὀγδόης Σεβαστῆς καὶ Πρώ/της Μινερουίας <Ε>ὐσεβοῦς Πιστῆς τετειμημέ/νον δώροις στρατιωτικοῖς ὑπὸ Αὐτοκράτο/ρος Νέρου […] |
| `Αὐρ(ηλίου)` | EDCS-00000274-0 | (not sub-classified) | Ἀγατῇ Τύχῃ // Ἔτους μσ´Σεβαστοῦ τοῦ καὶ βξτ´// γυμνασιαρχούσης τῆς πόλε<ω=Ο>ς / ἐφηβαρχοῦντος Αὐρ(ηλίου) Δημέα Ἀγ//α(θοκλέους) ἔφηβοι Κλα(ύδιος) Ἀκτικός Αὐρ(ήλιος) Ἀκυλιανὸς Θεμ//ίσ//ων / Σε{σ}πτίμιος Ἴλιος Αὐρ(ήλιος) Ἀρτεμείδωρος Φιδ//ίου / Αὐρ(ήλιος) Αρδουβαρις Αὐρ(ήλιος) Μένανδρος Αἰμι//λίου / Αὐ […] |

**Are these bilingual inscriptions?** Mostly not, and that settles it.

| measure | value | share |
| --- | --- | --- |
| inscriptions contributing a Greek-script drop | 5,569 |  |
| of those, also contributing a kept Latin pair | 66 | 1.19% |

A genuinely bilingual inscription would yield Latin pairs and Greek pairs from the same stone. The share that does is reported above. The remainder are Greek inscriptions that happen to live in EDCS, and Greek abbreviation practice is its own system — different formulae, different names, a different alphabet. Folding them in would not enrich the Latin task; it would silently average two tasks and make the ambiguity tables incoherent, since a Greek and a Latin abbreviation sharing a shape share nothing else.

They are worth keeping as a clearly separated Greek subset for anyone who wants that task, and worth keeping out of the Latin one.

**Bias check.** How the dropped pairs compare with the kept pairs.

| measure | value | reading |
| --- | --- | --- |
| province distribution (TVD) | 0.583 | a different population |
| century distribution (TVD) | 0.209 | materially different |
| median inscription length | 152 vs 119 kept | comparable |
| mean inscription length | 549 vs 280 kept |  |

Total variation distance: 0.00 means the dropped pairs are spread exactly like the kept ones, 1.00 means they share no common ground. Anything above about 0.15 is a materially different population.

Provinces, dropped share against kept share:

| province | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| Roma | 3,597 | 27.70% | 22.62% | 1.22 |
| Sicilia | 1,654 | 12.74% | 0.31% | 41.71 |
| Asia | 1,478 | 11.38% | 0.52% | 21.70 |
| Moesia inferior | 850 | 6.55% | 1.03% | 6.38 |
| Macedonia | 588 | 4.53% | 0.41% | 10.94 |
| Latium et Campania / Regio I | 581 | 4.47% | 8.79% | 0.51 |
| Bruttium et Lucania / Regio III | 564 | 4.34% | 0.49% | 8.91 |
| Thracia | 464 | 3.57% | 0.21% | 16.91 |

Centuries, dropped share against kept share (centuries below 0.5% of this category's drops omitted as noise):

| century | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| 5BC | 141 | 1.09% | 0.00% | 15463.79 |
| 3BC | 541 | 4.17% | 0.08% | 50.84 |
| 2BC | 139 | 1.07% | 0.26% | 4.11 |
| 1BC | 142 | 1.09% | 3.00% | 0.36 |
| 1AD | 555 | 4.27% | 13.33% | 0.32 |
| 2AD | 1,531 | 11.79% | 19.84% | 0.59 |
| 3AD | 1,637 | 12.60% | 9.96% | 1.26 |
| 4AD | 1,642 | 12.64% | 3.80% | 3.33 |
| 5AD | 312 | 2.40% | 0.87% | 2.77 |
| 6AD | 207 | 1.59% | 0.51% | 3.11 |

Abbreviation frequency profile of the dropped pairs, measured against how often each form survives in the kept set:

| profile | dropped pairs | share |
| --- | --- | --- |
| form never seen in the kept set | 0 | 0.00% |
| form seen fewer than 10 times | 0 | 0.00% |
| form seen 10+ times (already well covered) | 1 | 100.00% |

## 5. `unbalanced_parens` — 561

**40 examples.**

| raw token | inscription id | sub-class | inscription_text |
| --- | --- | --- | --- |
| `s(itus` | EDCS-00000031-0 | (not sub-classified) | Amynta / Vipstanorum / vilicus h(ic) s(itus est) |
| `leg(ionis` | EDCS-00000154-0 | (not sub-classified) | ] / [3]++ an[n(orum) ?] / [3] leg(ionis ?) [3] / [ |
| `q(ui` | EDCS-00000165-0 | (not sub-classified) | [D(is) M(anibus)] / Baleria Vi/ndemia Ata/nio Fyrmi/no coniugi / suo q(ui vixit) ann(os) XXXIII / m(enses) VI d(ies) VIII b(ene) m(erenti) / et sibi |
| `Ap(pius` | EDCS-00000179-0 | (not sub-classified) | [3] invicto / [co]nservatori / [I]mp(eratoris) senatus p(opuli)q(ue) R(omani) / leg(ionibus) VII Cl(audia) et IIII Fl(avia) / concordibus / P(ublius) Tur() Iulianus / et P(ublius) Tur() Proculus / et P(ublius) Sossius Antiochu[s / 3]QV[3] T(itus) F(la)v(ius) Maxiumu[s / e]t Ap(pius ?) Flavius B[3 /  […] |
| `\|(τοῦ` | EDCS-00000281-4 | (not sub-classified) | Μ(ᾶρκον) Αὐρήλιον Ἀντωνεῖνον Καίσαρα Αὐτοκράτορο[ς] Λ(ουκίου) Σεπτιμίου Σεουήρου Εὐσεβοῦς Περτίνακος [Σε]βαστ[οῦ] Ἀραβικοῦ Ἀδιαβηνικοῦ υἱόν ἡ βουλὴ καὶ [ὁ δῆμος] / ἐπὶ ἐπιμελητοῦ Σεραπίωνος \|(τοῦ Σεραπίωνοσ) Πατρο[ |
| `Tur(rani` | EDCS-00000321-0 | (not sub-classified) | Tur(rani ?) |
| `A(` | EDCS-00000546-0 | (not sub-classified) | C(ai?) A( ) |
| `Cleopatra(e` | EDCS-00000580-0 | (not sub-classified) | [Ny]mphis Salutarib(us) / M(arcus) A[ur(elius)] [M](arci) [Aur(elii)] lib(ertus) / Bellicianu[s] [---]lar / acens(!) it(e)r(um?) P(rovinciae) P(annoniae) S(uperioris) / pro salute sua et Iuliae / Cleopatra(e [coni]ug(is) e(t) / M(arci) Aur(elii) Belli[cia]ni fili(i) / v(otum) s(olvit) l(ibens) m(eri […] |
| `ped(itib(us)` | EDCS-00000639-1 | (not sub-classified) | Imp(erator) Caes(ar) divi H[adriani f(ilius) divi Traiani] / Parth(ici) n(epos) divi N[ervae pron(epos) T(itus) Aelius Ha]/drianus Anton[inus Aug(ustus) Pius p(ontifex) m(aximus) tr(ibunicia) po(testate)] / XVIIII imp(erator) II [co(n)s(ul) IV p(ater) p(atriae)] / eq(uitibus) et ped(itib(us) q(ui) m […] |
| `(` | EDCS-00000757-1 | (not sub-classified) | ] Ἀλέξανδρος / Εὐφράνορος δ(ραχμὰς) ε´ / Εἰσιδώρα / Σεραπίωνος δ(ραχμὰς) ε´ / Ζωσίμη Τερ- πωλίω ( vac. 1) δ(ραχμὰς) ε´ / Πόλων / Δωσιθέου δ(ραχμὰς) ε´ |
| `M(ani` | EDCS-00001054-0 | (not sub-classified) | ]nt(ius) M(ani Filius) / [3]gi [3] / [3]+ Pol(lia) pa(ter) / [ |
| `(centuria` | EDCS-00001236-1 | (not sub-classified) | (centuria P() P() Valeri / Sulli B() |
| `(sextarii` | EDCS-00001244-0 | (not sub-classified) | IV (sextarii ?) |
| `sal(utis` | EDCS-00001348-0 | (not sub-classified) | T(itus) Fl(avius) A[ug(usti) l(ibertus) He]rmes / anim[o 3]o sac/ro d[3]tali tig/num po[3]+ qui mul/tis aut<e=I>m [3 mem]ore sal(utis ?) / caput a[3 car]mine lae/to meor[um 3]idem d[ii] / immorta[les (?) 3]+itu[2] / dulci anim[ae (?) 3] / pulus g[3] / [1]eam[ |
| `(Milia` | EDCS-00001377-0 | (not sub-classified) | [D(omino)] n[ostro] / Caes(ari) / Theudo/[si]o Pio Feli/[ci] ac trium/[phat]ori semper / Aug(usto) m[un(icipium) Miza]eo/[there]na de/[v]o[t(um) n(umini)] eius / (Milia passuum) XIIIL |
| `Kal(en` | EDCS-00300242-0 | (not sub-classified) | Geddo fidel(is) in pa(ce) rece(ssit) III Kal(en/das) Octobres / Cypr(i)an(u)s fid(elis) in pa(ce) rece(ssit) V / Kal(endas) Iulias / Bonifatia fidel(is) in p(ace) rece(ssit) XVII / Kal(endas) Ianu(a) |
| `M(anib` | EDCS-00380165-0 | (not sub-classified) | D(is) M(anib/us) s(acrum) / Socellio / Ursion/i fili(i)s pa/tri bene / merenti |
| `b(ene` | EDCS-00600479-0 | (not sub-classified) | D(is) M(anibus) / P(ubli) F(lavi?) Clari / qui vix(it) an(nos) / XXV m(enses) VI d(ies) / VII P(ublius) F(lavius?) Mer/curius f(ratri) b(ene merenti) |
| `(ita` | EDCS-02500005-0 | (not sub-classified) | [DD]D(omini) nnn(ostri) Auggg(usti) Valen[t]inianus Valens Gratia[n]us [ave] Feste [carissime n]ob(is) / honorem Asiae ac totius provinci[a]e dignitatem quae ex iudicantis pendebat arbitrio [exe]mplo Illyri[c]i a[d]que Italarum urbium recte perpexi[mus(!)] / esse firmatum nec enim utile videbatur ut […] |
| `r(ei` | EDCS-02900102-0 | (not sub-classified) | DDD(ominis) nnn(ostris) FFF(lavis) / Valentinia/no Valenti / et Gratiano / semper Au/ggg(ustis) b(ono) r(ei publicae) nati(s) / [3] m(ilia) p(assuum) |
| `II(annorum` | EDCS-03000710-0 | (not sub-classified) | G(arum) scomb(ri) / flos / II(annorum duorum) / Cl(audi) Valeri |
| `Philargur(i` | EDCS-03300015-0 | (not sub-classified) | C(ai) Egnati L(uci) [f(ilii?)] / Philargur(i ) |
| `Fa(stidieni` | EDCS-03300118-0 | (not sub-classified) | L(uci) Fa(stidieni Optati) |
| `s(itus` | EDCS-04000638-0 | (not sub-classified) | Larci Here/dis Geminus / cum frat(re) h(ic) s(itus est) |
| `c(entum` | EDCS-04202447-0 | (not sub-classified) | Gavia Q(uinti) f(ilia) Maxima / in aquam HS q(uinque) c(entum milia) \|(centum milia) / [test]amento dedit |
| `\|(centum` | EDCS-04202447-0 | (not sub-classified) | Gavia Q(uinti) f(ilia) Maxima / in aquam HS q(uinque) c(entum milia) \|(centum milia) / [test]amento dedit |
| `p(e` | EDCS-04600084-0 | (not sub-classified) | L(ocus) m(onumenti) / q(u)oqu<o=E>/v(ersus) p(e/des) XXX / hic locus / meu(m) her(edem) / non seque(tur) |
| `\|(mulieris` | EDCS-05000204-0 | (not sub-classified) | Lepidia \|(mulieris ) l(iberta) / Bassa / Sex(ti) Riguli |
| `l(iberti` | EDCS-05400513-0 | (not sub-classified) | Nerito / Satri st(ationis) l(iberti servo) / [v]ilico summ(arum) / Segusione / [M]asculus soc(iorum) |
| `r(at` | EDCS-05401563-0 | (not sub-classified) | Flavia Optata mili(tis) de / num(ero) Regi(orum) emi(t) sib(i) de / r(e) v(iri) si quis pos(t) obit(um) / me(um) arc(am) volu(erit) ap(erire) infe/r(at fisci) vi(ribus) aur(i) lib(ram) una(m) |
| `r(ei` | EDCS-05500363-0 | (not sub-classified) | D(is) M(anibus) s(acrum) / Sulpiciae Col/lippone(n)si an(norum) / XXXV Gallaecus / r(ei publicae) s(uae) l(ibertus) uxori / p(ientissimae) p(onendum) c(uravit) |
| `r(ei` | EDCS-05501139-0 | (not sub-classified) | posu]/it Gabinius / Mucro c(urator?) r(ei publicae?) / c(oloniae) U(lpiae) Italicensi/um |
| `l(ibertae` | EDCS-05503624-0 | (not sub-classified) | L(ucio) Rufidio One/simo et Ruf(idiae) / Euche uxori et / Ruf(idiae) Fortunatae l(ibertae) / et Ruf(idiae) Festivae l(ibertae ) et L(ucio) Ruf(idio) / Fortunato l(iberto) |
| `r(ei` | EDCS-05600157-0 | (not sub-classified) | D(omino) n(ostro) Magno / Magnenti/o P(io) Invic(to) sem/p(er) Aug(usto) / b(ono) r(ei publicae) n(ato) |
| `XX(centena` | EDCS-05700098-0 | (not sub-classified) | Imp(erator) Caesar divi Hadriani fil(ius) divi Traiani Parthici nep(os) divi N[ervae] / pronepos T(itus) Aelius Hadrianus Antoninus Aug(ustus) Pius pontif(ex) max(imus) trib(unicia) potes[t(ate) II co(n)s(ul) II] / thermas in quarum exstructionem div<u=O>s pater suus HS XX(centena milia) pollici[tus […] |
| `\|(dextantem` | EDCS-05700668-0 | (not sub-classified) | L(ucius) Aurelius L(uci) f(ilius) Pal(atina) / Priscus / fecit sibi et / L(ucio) Aurelio Felici p(atri) / Cloeliae Sex(ti) f(iliae) / Priscae mat(ri) / Aureliae L(uci) f(iliae) / Priscae f(iliae) / L(ucio) Aurelio L(uci) l(iberto) Euplo / L(ucio) Aurelio L(uci) l(iberto) Floro / L(ucio) Aurelio L(uc […] |
| `be(ne` | EDCS-05701694-0 | (not sub-classified) | [Q]ui(!) vixit annis viginti mensibus / [quatt]uor horis duodecim(is) qui ex his / [mecum a]nno uno mensibus quinque die(bus) / [3]ius Tiberinus coniugi be(ne merenti) |
| `Σαράπι(δι` | EDCS-05800883-1 | (not sub-classified) | Γ(άϊος) Βαλέριος Ἑρμαίσκος ἐποί[ησεν τὸ] / Σαραπεῖον Διὶ Ἡλίῳ μεγάλῳ / Σαράπι(δι κα)ὶ τοῖς συννάοις θεοῖς |
| `prim(us` | EDCS-05801906-0 | (not sub-classified) | C(aius) Apidius P(ubli) f(ilius) Qui(rina) Bassus prim(us pilus) / leg(ionis) XI VIIIvir Amiterni / ex testamento factum praeter locum HS C / arbitratu / Q(uinti) Orfi Q(uinti) f(ilii) Qui(rina) Flacci Caesi et / Q(uinti) Porci Q(uinti) f(ilii) Serg(ia) Sabini et / Nygmi l(iberti) |
| `(milia` | EDCS-06000961-0 | (not sub-classified) | Imp(erator) C(aius) Iulius / Verus Maximinus / Pius Felix [Aug(ustus)] / erm[ani]/cus maximus Sarmati/cus maxim[u]s Dacicus / maximus tribuniciae potestatis [ter(tium)] / imp(erator) et C(aius) Iul[iu]s Verus Maxi/mus n[o]bil[is]s[imus] Caes(ar) prince[ps] / iuven[t]uti[s] Germanicus max[i]/ṃus [Sa […] |

**This is not damaged stone. It is a tokenizer bug.**

EDCS lets one abbreviation expand to more than one word: `h(ic) s(itus est)`, `q(ui vixit)`, `b(ene merenti)`. The expansion contains a space, the probe splits tokens on whitespace, and the parenthesis is severed — the opening half lands here as unbalanced while the closing half is discarded silently for having no `(` at all.

| measure | value | note |
| --- | --- | --- |
| genuine multi-word expansions in the corpus | 442 |  |
| distinct multi-word expansions | 268 |  |
| of those, Latin script (recoverable for this dataset) | 338 | 189 distinct |

Most frequent multi-word expansions:

| expansion | occurrences |
| --- | --- |
| `(ene merenti)` | 28 |
| `(ei publicae)` | 19 |
| `(e merenti)` | 12 |
| `(centena milia)` | 10 |
| `(principis prioris)` | 8 |
| `(it annos)` | 6 |
| `(itus est)` | 5 |
| `(vicies centenis milibus)` | 5 |
| `(pili posterioris)` | 5 |
| `(e se)` | 5 |
| `(ita est)` | 5 |
| `(Hastata posterior)` | 5 |
| `(i quis)` | 4 |
| `(undecies centenis)` | 4 |
| `(hastati posterioris)` | 4 |

These are recoverable exactly and cheaply, by closing the parenthesis before splitting on whitespace. They are also the single-letter, high-frequency funerary formulae — the most common abbreviations in the corpus — so losing them removes real one-to-many cases from a dataset whose whole purpose is learning how abbreviations expand.

**Bias check.** How the dropped pairs compare with the kept pairs.

| measure | value | reading |
| --- | --- | --- |
| province distribution (TVD) | 0.197 | materially different |
| century distribution (TVD) | 0.082 | mild skew |
| median inscription length | 132 vs 119 kept | comparable |
| mean inscription length | 370 vs 280 kept |  |

Total variation distance: 0.00 means the dropped pairs are spread exactly like the kept ones, 1.00 means they share no common ground. Anything above about 0.15 is a materially different population.

Provinces, dropped share against kept share:

| province | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| Roma | 97 | 17.29% | 22.62% | 0.76 |
| Latium et Campania / Regio I | 52 | 9.27% | 8.79% | 1.05 |
| Numidia | 36 | 6.42% | 7.72% | 0.83 |
| Africa proconsularis | 27 | 4.81% | 8.57% | 0.56 |
| Moesia inferior | 25 | 4.46% | 1.03% | 4.34 |
| Venetia et Histria / Regio X | 20 | 3.57% | 3.42% | 1.04 |
| Germania superior | 19 | 3.39% | 2.68% | 1.26 |
| Hispania citerior | 17 | 3.03% | 3.18% | 0.95 |

Centuries, dropped share against kept share (centuries below 0.5% of this category's drops omitted as noise):

| century | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| 1AD | 52 | 9.27% | 13.33% | 0.70 |
| 2AD | 95 | 16.93% | 19.84% | 0.85 |
| 3AD | 73 | 13.01% | 9.96% | 1.31 |
| 4AD | 30 | 5.35% | 3.80% | 1.41 |

Abbreviation frequency profile of the dropped pairs, measured against how often each form survives in the kept set:

| profile | dropped pairs | share |
| --- | --- | --- |
| form never seen in the kept set | 2 | 14.29% |
| form seen fewer than 10 times | 2 | 14.29% |
| form seen 10+ times (already well covered) | 10 | 71.43% |

## 6. `no_letters_outside_parens` — 469

**Sub-classification.** One label was hiding several situations.

| sub-class | count | share of category |
| --- | --- | --- |
| Greek word fully supplied by the editor | 321 | 68.44% |
| Latin word fully supplied by the editor | 145 | 30.92% |
| other | 3 | 0.64% |

**40 examples.**

| raw token | inscription id | sub-class | inscription_text |
| --- | --- | --- | --- |
| `(filium)` | EDCS-00000065-0 | Latin word fully supplied by the editor | [3]VEGONTATAT filia Pluto/ni com(m)endata / Domitium Tarui (filium) / dibus Manibus / et P(l)utoni |
| `(filius)` | EDCS-00000066-0 | Latin word fully supplied by the editor | Carus / Vuaci (filius) / dibus (M)an(i)bus / com(m)en/datus |
| `(filius)` | EDCS-00000067-0 | Latin word fully supplied by the editor | Domitius / Tarui (filius) et / Laties filia / ab in[f]eros EN / [ |
| `(δηνάρια)` | EDCS-00000283-0 | Greek word fully supplied by the editor | Ἰησ(οῦς) Α Ω // Αὐρ(ήλιος) Ναζάρις / ζῶν έαυτῶι κατε/σκεύασα τὸ κυμη/τήριον καὶ τοῖς ἀ/δελφίοις καὶ τῇ μη/τρί μου Ἐπικτήσι / μετὰ δὲ τὸ κατατε/θῆναι τὴν μητέρα / μου εἴ τις ἂν σκυλῇ / δώσει τῷ ταμείῳ (δηνάρια) / αφ´καὶ ἐν ἐκείνῃ / τῇ ἡμέρᾳ δώσει λό/γον τῷ θεῷ |
| `(δηνάρια)` | EDCS-00000286-0 | Greek word fully supplied by the editor | Αὐρ(ήλιος) Σωσιανὸς Ἀσκληπιόδοτος / ἀνενεωσάμην τὴν προγονικήν μου πύελον ἐμαυτῷ καὶ τῆ συνβίῳ μου Αὐρ(ηλιᾳ) / Χρηστείνῃ / Αὐρ(ήλιος) Ἀσκληπιόδοτος ὁ κὲ Παρηγόριος ἀρχιτέκτων φύλαρ(χος) τῆς κρα(τίστης) φυλῆς / Ματιδιανῆς ἀνενεωσάμην τὴν τῶν γονέων μου πύαλον καὶ ἐπέγραψα / ἐμαυτῷ καὶ τῷ γλυκυτάτῃ μο […] |
| `(δηνάρια)` | EDCS-00000286-0 | Greek word fully supplied by the editor | Αὐρ(ήλιος) Σωσιανὸς Ἀσκληπιόδοτος / ἀνενεωσάμην τὴν προγονικήν μου πύελον ἐμαυτῷ καὶ τῆ συνβίῳ μου Αὐρ(ηλιᾳ) / Χρηστείνῃ / Αὐρ(ήλιος) Ἀσκληπιόδοτος ὁ κὲ Παρηγόριος ἀρχιτέκτων φύλαρ(χος) τῆς κρα(τίστης) φυλῆς / Ματιδιανῆς ἀνενεωσάμην τὴν τῶν γονέων μου πύαλον καὶ ἐπέγραψα / ἐμαυτῷ καὶ τῷ γλυκυτάτῃ μο […] |
| `(δηνάρια)` | EDCS-00000312-1 | Greek word fully supplied by the editor | [τοῦτ]ο τὸ ἡρῷον καὶ ἡ ἐπικει/[μένη] σορός ἐστιν Αὐρ(ηλίου) Ἀρισ/[τοκλ]έους καὶ θυγατὸς αὐτοῦ / Ἀριστοκλείης καὶ τέκνων / [αὐτ]ῶν ζῶσιν εἰ δέ τις ἕ/[τερ]ος βάλῃ πτῶμα ἢ γράμ/[μα ἐκ]κόψει δώσει τῷ ταμεί/[ῳ] (δηνάρια) μ(ύρια) λαμπρά |
| `(δηνάρια)` | EDCS-00000335-0 | Greek word fully supplied by the editor | στεφανηφορἠσασαν φιλο]/τείμως καὶ γυμνασιαρχ/ήσασαν καὶ δοῦσαν τὰ ἐξ ἔ/θους φιλάνθρωπα παρ ὅ/λον τὸ ἔτος καὶ ἐν ἀρχῇ ἐκ / πλήρους τῇ τε βουλῇ καὶ τῇ / γερουσίᾳ ἐν κατακλίσει καὶ / τοῖς νέοις εἰσκομίσασαν / τῇ πόλει τὰ τοῦ δείπνου / διδόμενα τῆς στεφανη/φορίας (δηνάρια) μ(ύρι)α ὧν κατεσκευ/άσθη ὁ περ […] |
| `(δηνάρια)` | EDCS-00000335-0 | Greek word fully supplied by the editor | στεφανηφορἠσασαν φιλο]/τείμως καὶ γυμνασιαρχ/ήσασαν καὶ δοῦσαν τὰ ἐξ ἔ/θους φιλάνθρωπα παρ ὅ/λον τὸ ἔτος καὶ ἐν ἀρχῇ ἐκ / πλήρους τῇ τε βουλῇ καὶ τῇ / γερουσίᾳ ἐν κατακλίσει καὶ / τοῖς νέοις εἰσκομίσασαν / τῇ πόλει τὰ τοῦ δείπνου / διδόμενα τῆς στεφανη/φορίας (δηνάρια) μ(ύρι)α ὧν κατεσκευ/άσθη ὁ περ […] |
| `(δηνἀρια)` | EDCS-00000335-0 | Greek word fully supplied by the editor | στεφανηφορἠσασαν φιλο]/τείμως καὶ γυμνασιαρχ/ήσασαν καὶ δοῦσαν τὰ ἐξ ἔ/θους φιλάνθρωπα παρ ὅ/λον τὸ ἔτος καὶ ἐν ἀρχῇ ἐκ / πλήρους τῇ τε βουλῇ καὶ τῇ / γερουσίᾳ ἐν κατακλίσει καὶ / τοῖς νέοις εἰσκομίσασαν / τῇ πόλει τὰ τοῦ δείπνου / διδόμενα τῆς στεφανη/φορίας (δηνάρια) μ(ύρι)α ὧν κατεσκευ/άσθη ὁ περ […] |
| `(δηνάρια)` | EDCS-00000357-0 | Greek word fully supplied by the editor | Παῦλος μετὰ / τῆς γυνεκὸς / Πρόκλης ζῶντες / φρονοῦντες ἐποι/ήσαμεν ἑαυτοῖς / κοιμητήριον καὶ / ἀνεστήσαμεν / στἠλην μετὰ τὴν / κατάθεσιν τῶν / δύο ἀξιοῦμεν μ/ηδένα ἐπανῦξε / ἢ τόνδ᾽ υἱόν μου Ἁδρι/ανόν ἑὰν δέ τις ἓτερο<ς> / ἐπανύξῃ δώσ<ε>ι τῷ / ταμίῳ προστίμου / (δηνάρια) δφ |
| `(δηνάρια)` | EDCS-00000425-0 | Greek word fully supplied by the editor | τὸ μνημεῖον κατεσκεύασεν Αὐρ(ήλιος) Ερ[μ3 Πιλο]/κράτους Κορυδαλλεὺς ἀπὸ Πυγέλων ἑαυ[τῷ καὶ γυναικὶ] / καὶ μητρὶ αὐτοῦ Αφφιᾳ καὶ ἀδελφοῖς μου Αὐρ(ηλίοις) Λέο[ντι καὶ] / Φιλοκράτῃ καὶ ἀνεψιῷ μου Ἀρκολέοντι ἄλλ[ῳ δὲ οὐδενὶ ἐξέσται] / κηδευθῆναι ἢ ὁ βιασάμενος ἐνθάψαι τινὰ κα[ὶ ἔνοχος ἔσται ἐκ το]/ῦ τυν […] |
| `(δηνάρια)` | EDCS-00000457-0 | Greek word fully supplied by the editor | Κλ(αυδία) Ἀννα τὴν σωματοθήκην ἑαυτῇ καὶ Συριάρχῃ / καὶ υἱῷ Ἑρμιανῷ καὶ τῇ ἀδελφῇ Αὐρ(ηλίᾳ) Οα / καὶ τῷ προενειμέ/νῳ αὐτῆς ἀν/δρί ἄλλῳ δὲ οὐ/δενὶ ἔξεσται / ἐπιθάψαι τινά / ἐπεὶ δώσει προσ/τείμου Διὶ Σολυμεῖ (δηνάρια) αφ |
| `(ἔτους)` | EDCS-00000510-0 | Greek word fully supplied by the editor | θεοῖς Σωτῆρσι τὸ κολλήγιον ὑπὲρ / εὐσεβίας χάριν ἐπ ἀγαθῷ (ἔτους) β παχ(ὼν) ι |
| `(καὶ)` | EDCS-00000536-0 | Greek word fully supplied by the editor | ἐπὶ τοῦ εὐλαβ(εστάτου) θεοφιλ(εστάτου) Πομπ<η>ιανοῦ π/ρεσβ(υτέρου) (καὶ) Εἰκώβου φροντι<σ>τοῦ (καὶ) Εἰσακίου / διακώνου ἐκτίσθι (καὶ) ἐψηφώθε <ἡ> ἁγία ἐ/κκλησία μη(νὶ) Ἀρτη<μ>ισίο ἰνδ(ικτιῶνος) ἕκτη/ς τοῦ αξφ´ἔτους κύριε μνησ/θ<ητ>ῖ τοῦ καρποφ<ο=Ω>ρ<ή=Ε>σαντ<ο=Α>ς |
| `(καὶ)` | EDCS-00000536-0 | Greek word fully supplied by the editor | ἐπὶ τοῦ εὐλαβ(εστάτου) θεοφιλ(εστάτου) Πομπ<η>ιανοῦ π/ρεσβ(υτέρου) (καὶ) Εἰκώβου φροντι<σ>τοῦ (καὶ) Εἰσακίου / διακώνου ἐκτίσθι (καὶ) ἐψηφώθε <ἡ> ἁγία ἐ/κκλησία μη(νὶ) Ἀρτη<μ>ισίο ἰνδ(ικτιῶνος) ἕκτη/ς τοῦ αξφ´ἔτους κύριε μνησ/θ<ητ>ῖ τοῦ καρποφ<ο=Ω>ρ<ή=Ε>σαντ<ο=Α>ς |
| `(καὶ)` | EDCS-00000536-0 | Greek word fully supplied by the editor | ἐπὶ τοῦ εὐλαβ(εστάτου) θεοφιλ(εστάτου) Πομπ<η>ιανοῦ π/ρεσβ(υτέρου) (καὶ) Εἰκώβου φροντι<σ>τοῦ (καὶ) Εἰσακίου / διακώνου ἐκτίσθι (καὶ) ἐψηφώθε <ἡ> ἁγία ἐ/κκλησία μη(νὶ) Ἀρτη<μ>ισίο ἰνδ(ικτιῶνος) ἕκτη/ς τοῦ αξφ´ἔτους κύριε μνησ/θ<ητ>ῖ τοῦ καρποφ<ο=Ω>ρ<ή=Ε>σαντ<ο=Α>ς |
| `(καὶ)` | EDCS-00000537-0 | Greek word fully supplied by the editor | ]ΟΥ πρεσβ(υτέρου) [3]/ΦΡΕΚΑ[3 ἐψη]φ<ώ=Ε>θ<η=Ι> ὁ τόπος τ<ῶ=Ο>ν ἁγίων μ[α]/ρτύρ<ω=Ο>ν ἐξ ἰδίων Ἰακώβου φρο<ν>τ(ιστοῦ) (καὶ) Αβωεις |
| `(ἔτους)` | EDCS-00000554-0 | Greek word fully supplied by the editor | [Διέγραψ(εν)] Πετορ(ζμῆθις) Βιήν(χιος) / [ὑπ(ὲρ) λαογρ]αφ(ίας) ιδ´ (ἔτους) Δομιτιανοῦ / [Καίσαρος τ]οῦ κυρίου ἐπὶ λ(όγου) (δραχμὰς) η´ / [ὁ αὐτὸς τὰς] λοιπ(ὰς) (δραχμὰς) θ´ (γίνονται) (δραχμαὶ) ιζ´ / [Σωκράτης] πράκ(τωρ) ἔγραψ(α) / [(ἔτους) ιε´ Δ]ομιτιανοῦ Καίσαρος / [τοῦ κυρί]ου Φαῶφι ι´ |
| `(δραχμὰς)` | EDCS-00000554-0 | Greek word fully supplied by the editor | [Διέγραψ(εν)] Πετορ(ζμῆθις) Βιήν(χιος) / [ὑπ(ὲρ) λαογρ]αφ(ίας) ιδ´ (ἔτους) Δομιτιανοῦ / [Καίσαρος τ]οῦ κυρίου ἐπὶ λ(όγου) (δραχμὰς) η´ / [ὁ αὐτὸς τὰς] λοιπ(ὰς) (δραχμὰς) θ´ (γίνονται) (δραχμαὶ) ιζ´ / [Σωκράτης] πράκ(τωρ) ἔγραψ(α) / [(ἔτους) ιε´ Δ]ομιτιανοῦ Καίσαρος / [τοῦ κυρί]ου Φαῶφι ι´ |
| `(δραχμὰς)` | EDCS-00000554-0 | Greek word fully supplied by the editor | [Διέγραψ(εν)] Πετορ(ζμῆθις) Βιήν(χιος) / [ὑπ(ὲρ) λαογρ]αφ(ίας) ιδ´ (ἔτους) Δομιτιανοῦ / [Καίσαρος τ]οῦ κυρίου ἐπὶ λ(όγου) (δραχμὰς) η´ / [ὁ αὐτὸς τὰς] λοιπ(ὰς) (δραχμὰς) θ´ (γίνονται) (δραχμαὶ) ιζ´ / [Σωκράτης] πράκ(τωρ) ἔγραψ(α) / [(ἔτους) ιε´ Δ]ομιτιανοῦ Καίσαρος / [τοῦ κυρί]ου Φαῶφι ι´ |
| `(γίνονται)` | EDCS-00000554-0 | Greek word fully supplied by the editor | [Διέγραψ(εν)] Πετορ(ζμῆθις) Βιήν(χιος) / [ὑπ(ὲρ) λαογρ]αφ(ίας) ιδ´ (ἔτους) Δομιτιανοῦ / [Καίσαρος τ]οῦ κυρίου ἐπὶ λ(όγου) (δραχμὰς) η´ / [ὁ αὐτὸς τὰς] λοιπ(ὰς) (δραχμὰς) θ´ (γίνονται) (δραχμαὶ) ιζ´ / [Σωκράτης] πράκ(τωρ) ἔγραψ(α) / [(ἔτους) ιε´ Δ]ομιτιανοῦ Καίσαρος / [τοῦ κυρί]ου Φαῶφι ι´ |
| `(δραχμαὶ)` | EDCS-00000554-0 | Greek word fully supplied by the editor | [Διέγραψ(εν)] Πετορ(ζμῆθις) Βιήν(χιος) / [ὑπ(ὲρ) λαογρ]αφ(ίας) ιδ´ (ἔτους) Δομιτιανοῦ / [Καίσαρος τ]οῦ κυρίου ἐπὶ λ(όγου) (δραχμὰς) η´ / [ὁ αὐτὸς τὰς] λοιπ(ὰς) (δραχμὰς) θ´ (γίνονται) (δραχμαὶ) ιζ´ / [Σωκράτης] πράκ(τωρ) ἔγραψ(α) / [(ἔτους) ιε´ Δ]ομιτιανοῦ Καίσαρος / [τοῦ κυρί]ου Φαῶφι ι´ |
| `(δραχμὰς)` | EDCS-00000555-0 | Greek word fully supplied by the editor | Διέγραψ(εν) Ψαν[σνὼς] / πρεσβύτερος Πε[τροσζ(μήθιος)] / ὑπ(ὲρ) λαογραφ(ίας) πρ[ώτου (ἔτους)] / Νέρουα Καίσαρος [τοῦ κυρίου ἐπὶ λ(όγου)] / (δραχμὰς) ὀκτὼ Παχών ιγ´ [Ἑρμογ(ένης) πράκ(τωρ)] / ἔγραψα ὁ αὐτὸς Ψαν[σνὼς τὰς λοιπ(ὰς)] / τοῦ α´ (ἔτους) (δραχμὰς) ἐννήα (γίνονται) (δραχμαὶ) θ´ Ἑρμογ(ένης) [πρά […] |
| `(ἔτους)` | EDCS-00000555-0 | Greek word fully supplied by the editor | Διέγραψ(εν) Ψαν[σνὼς] / πρεσβύτερος Πε[τροσζ(μήθιος)] / ὑπ(ὲρ) λαογραφ(ίας) πρ[ώτου (ἔτους)] / Νέρουα Καίσαρος [τοῦ κυρίου ἐπὶ λ(όγου)] / (δραχμὰς) ὀκτὼ Παχών ιγ´ [Ἑρμογ(ένης) πράκ(τωρ)] / ἔγραψα ὁ αὐτὸς Ψαν[σνὼς τὰς λοιπ(ὰς)] / τοῦ α´ (ἔτους) (δραχμὰς) ἐννήα (γίνονται) (δραχμαὶ) θ´ Ἑρμογ(ένης) [πρά […] |
| `(δραχμὰς)` | EDCS-00000555-0 | Greek word fully supplied by the editor | Διέγραψ(εν) Ψαν[σνὼς] / πρεσβύτερος Πε[τροσζ(μήθιος)] / ὑπ(ὲρ) λαογραφ(ίας) πρ[ώτου (ἔτους)] / Νέρουα Καίσαρος [τοῦ κυρίου ἐπὶ λ(όγου)] / (δραχμὰς) ὀκτὼ Παχών ιγ´ [Ἑρμογ(ένης) πράκ(τωρ)] / ἔγραψα ὁ αὐτὸς Ψαν[σνὼς τὰς λοιπ(ὰς)] / τοῦ α´ (ἔτους) (δραχμὰς) ἐννήα (γίνονται) (δραχμαὶ) θ´ Ἑρμογ(ένης) [πρά […] |
| `(γίνονται)` | EDCS-00000555-0 | Greek word fully supplied by the editor | Διέγραψ(εν) Ψαν[σνὼς] / πρεσβύτερος Πε[τροσζ(μήθιος)] / ὑπ(ὲρ) λαογραφ(ίας) πρ[ώτου (ἔτους)] / Νέρουα Καίσαρος [τοῦ κυρίου ἐπὶ λ(όγου)] / (δραχμὰς) ὀκτὼ Παχών ιγ´ [Ἑρμογ(ένης) πράκ(τωρ)] / ἔγραψα ὁ αὐτὸς Ψαν[σνὼς τὰς λοιπ(ὰς)] / τοῦ α´ (ἔτους) (δραχμὰς) ἐννήα (γίνονται) (δραχμαὶ) θ´ Ἑρμογ(ένης) [πρά […] |
| `(δραχμαὶ)` | EDCS-00000555-0 | Greek word fully supplied by the editor | Διέγραψ(εν) Ψαν[σνὼς] / πρεσβύτερος Πε[τροσζ(μήθιος)] / ὑπ(ὲρ) λαογραφ(ίας) πρ[ώτου (ἔτους)] / Νέρουα Καίσαρος [τοῦ κυρίου ἐπὶ λ(όγου)] / (δραχμὰς) ὀκτὼ Παχών ιγ´ [Ἑρμογ(ένης) πράκ(τωρ)] / ἔγραψα ὁ αὐτὸς Ψαν[σνὼς τὰς λοιπ(ὰς)] / τοῦ α´ (ἔτους) (δραχμὰς) ἐννήα (γίνονται) (δραχμαὶ) θ´ Ἑρμογ(ένης) [πρά […] |
| `(ἔτους)` | EDCS-00000556-0 | Greek word fully supplied by the editor | Σωκρατίων καὶ οἱ λοιπ(οὶ) πράκ(τορες) χειρο(ναξίου) μη(νιαίου) Σο(ηνης) δι(έγραψεν) / Πετορζμῆθις Ἀραβρωότις ὑπ(ὲρ) χειρο(ναξίου) τὰς λοιπ(ὰς) τὰ ἕως Μεσορὴ λ´ τοῦ ε´ (ἔτους) Ἁδριανοῦ / Καίσαρος τοῦ κυρίου δραχ(μὰς) τέσσαρες (γίνονται) (δραχμαὶ) δ´ / Ἀμμώνιος ἔγραψα Μεσορὴ ις´ |
| `(γίνονται)` | EDCS-00000556-0 | Greek word fully supplied by the editor | Σωκρατίων καὶ οἱ λοιπ(οὶ) πράκ(τορες) χειρο(ναξίου) μη(νιαίου) Σο(ηνης) δι(έγραψεν) / Πετορζμῆθις Ἀραβρωότις ὑπ(ὲρ) χειρο(ναξίου) τὰς λοιπ(ὰς) τὰ ἕως Μεσορὴ λ´ τοῦ ε´ (ἔτους) Ἁδριανοῦ / Καίσαρος τοῦ κυρίου δραχ(μὰς) τέσσαρες (γίνονται) (δραχμαὶ) δ´ / Ἀμμώνιος ἔγραψα Μεσορὴ ις´ |
| `(δραχμαὶ)` | EDCS-00000556-0 | Greek word fully supplied by the editor | Σωκρατίων καὶ οἱ λοιπ(οὶ) πράκ(τορες) χειρο(ναξίου) μη(νιαίου) Σο(ηνης) δι(έγραψεν) / Πετορζμῆθις Ἀραβρωότις ὑπ(ὲρ) χειρο(ναξίου) τὰς λοιπ(ὰς) τὰ ἕως Μεσορὴ λ´ τοῦ ε´ (ἔτους) Ἁδριανοῦ / Καίσαρος τοῦ κυρίου δραχ(μὰς) τέσσαρες (γίνονται) (δραχμαὶ) δ´ / Ἀμμώνιος ἔγραψα Μεσορὴ ις´ |
| `(ἔτους)` | EDCS-00000557-0 | Greek word fully supplied by the editor | Πετροζμῆθις πράκ(τωρ) Ἐλεφ(αντίνης) διέ/γρα(ψεν) Γερμανὸς Μαικιανοῦ ὑπ(ὲρ) / τειμῆς δημοσίο(υ) φοίνικ(ος) γενήμ(ατος) / ιζ´ (ἔτους) ῥυπ(αρὰς) (δραχμὰς) δ´ (δίχαλκον) (ἔτους) ιη´Ἁδριανοῦ / τοῦ κυρίου Φαῶφι κβ´ |
| `(δραχμὰς)` | EDCS-00000557-0 | Greek word fully supplied by the editor | Πετροζμῆθις πράκ(τωρ) Ἐλεφ(αντίνης) διέ/γρα(ψεν) Γερμανὸς Μαικιανοῦ ὑπ(ὲρ) / τειμῆς δημοσίο(υ) φοίνικ(ος) γενήμ(ατος) / ιζ´ (ἔτους) ῥυπ(αρὰς) (δραχμὰς) δ´ (δίχαλκον) (ἔτους) ιη´Ἁδριανοῦ / τοῦ κυρίου Φαῶφι κβ´ |
| `(δίχαλκον)` | EDCS-00000557-0 | Greek word fully supplied by the editor | Πετροζμῆθις πράκ(τωρ) Ἐλεφ(αντίνης) διέ/γρα(ψεν) Γερμανὸς Μαικιανοῦ ὑπ(ὲρ) / τειμῆς δημοσίο(υ) φοίνικ(ος) γενήμ(ατος) / ιζ´ (ἔτους) ῥυπ(αρὰς) (δραχμὰς) δ´ (δίχαλκον) (ἔτους) ιη´Ἁδριανοῦ / τοῦ κυρίου Φαῶφι κβ´ |
| `(ἔτους)` | EDCS-00000557-0 | Greek word fully supplied by the editor | Πετροζμῆθις πράκ(τωρ) Ἐλεφ(αντίνης) διέ/γρα(ψεν) Γερμανὸς Μαικιανοῦ ὑπ(ὲρ) / τειμῆς δημοσίο(υ) φοίνικ(ος) γενήμ(ατος) / ιζ´ (ἔτους) ῥυπ(αρὰς) (δραχμὰς) δ´ (δίχαλκον) (ἔτους) ιη´Ἁδριανοῦ / τοῦ κυρίου Φαῶφι κβ´ |
| `(ἔτους)` | EDCS-00000559-0 | Greek word fully supplied by the editor | Μάκερ Διδύ(μου) καὶ Ἀμμών[3] / πράκ(τορες) ἀργ(uρικῶν) Ἐλεφ(αντίνης) δι(ὰ) Ἀμμωνί(ου) / διέγρ(αψεν) Παχνουβις Παχνουβ(ιος) / μητ(ρος) Σεραπ() υπ(ὲρ) μερισ(μοῦ) θ´ (ἔτους) δραχ(μὰς) / τέσσαρες (δραχμαὶ) δ´ (ἔτους) θ´ Παῦνι α´ |
| `(δραχμαὶ)` | EDCS-00000559-0 | Greek word fully supplied by the editor | Μάκερ Διδύ(μου) καὶ Ἀμμών[3] / πράκ(τορες) ἀργ(uρικῶν) Ἐλεφ(αντίνης) δι(ὰ) Ἀμμωνί(ου) / διέγρ(αψεν) Παχνουβις Παχνουβ(ιος) / μητ(ρος) Σεραπ() υπ(ὲρ) μερισ(μοῦ) θ´ (ἔτους) δραχ(μὰς) / τέσσαρες (δραχμαὶ) δ´ (ἔτους) θ´ Παῦνι α´ |
| `(ἔτους)` | EDCS-00000559-0 | Greek word fully supplied by the editor | Μάκερ Διδύ(μου) καὶ Ἀμμών[3] / πράκ(τορες) ἀργ(uρικῶν) Ἐλεφ(αντίνης) δι(ὰ) Ἀμμωνί(ου) / διέγρ(αψεν) Παχνουβις Παχνουβ(ιος) / μητ(ρος) Σεραπ() υπ(ὲρ) μερισ(μοῦ) θ´ (ἔτους) δραχ(μὰς) / τέσσαρες (δραχμαὶ) δ´ (ἔτους) θ´ Παῦνι α´ |
| `(centuria)` | EDCS-00000568-0 | Latin word fully supplied by the editor | M(arcus) Camurius / M(arci) f(ilius) Pol(lia) Fortis / Regio mil(es) / coh(ortis) X urb(anae) / (centuria) Viri / militavit annos VIII / vix(it) ann(os) XXV |
| `(miliaria)` | EDCS-00000581-0 | Latin word fully supplied by the editor | [Imp(erator) Caes(ar) divi Hadriani f(ilius) divi Traian(i) Parthic(i) n(epos) divi Nervae pron(epos) T(itus) Aelius Hadrianus Antoninus Aug(ustus) Pius pont(ifex) max(imus) tr(ibunicia) pot(estate) XIV–XVI(?) imp(erator) II co(n)s(ul) IV p(ater) p(atriae)] / [equit(ibus) et pedit(ibus) qui milit(av […] |

**Confirmed, correctly dropped.** `(filius)` with nothing outside the parenthesis is a word the editor supplied in full. There is no abbreviation: the stone shows nothing here, so there is no surface form to expand and no input side to the training pair. Keeping them would teach a model to hallucinate words out of empty space.

Worth noting that two thirds of this category is Greek rather than Latin, so most of it would fall out of the Latin dataset on script grounds anyway.

**Bias check.** How the dropped pairs compare with the kept pairs.

| measure | value | reading |
| --- | --- | --- |
| province distribution (TVD) | 0.497 | a different population |
| century distribution (TVD) | 0.142 | mild skew |
| median inscription length | 177 vs 119 kept | dropped pairs come from longer, more damaged texts |
| mean inscription length | 746 vs 280 kept |  |

Total variation distance: 0.00 means the dropped pairs are spread exactly like the kept ones, 1.00 means they share no common ground. Anything above about 0.15 is a materially different population.

Provinces, dropped share against kept share:

| province | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| Roma | 86 | 18.34% | 22.62% | 0.81 |
| Aegyptus | 67 | 14.29% | 0.30% | 48.01 |
| Sicilia | 43 | 9.17% | 0.31% | 30.03 |
| Latium et Campania / Regio I | 34 | 7.25% | 8.79% | 0.82 |
| Asia | 25 | 5.33% | 0.52% | 10.17 |
| Achaia | 15 | 3.20% | 0.22% | 14.24 |
| Syria | 14 | 2.99% | 0.49% | 6.06 |
| Arabia | 14 | 2.99% | 0.33% | 9.11 |

Centuries, dropped share against kept share (centuries below 0.5% of this category's drops omitted as noise):

| century | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| 1AD | 47 | 10.02% | 13.33% | 0.75 |
| 2AD | 51 | 10.87% | 19.84% | 0.55 |
| 3AD | 40 | 8.53% | 9.96% | 0.86 |
| 4AD | 29 | 6.18% | 3.80% | 1.63 |

## 7. `token_carries_markup` — 339

**40 examples.**

| raw token | inscription id | sub-class | inscription_text |
| --- | --- | --- | --- |
| `Αὐρ(ηλίου)]` | EDCS-00000565-0 | (not sub-classified) | θέμιδος τετ]ραετη/[ρικῆς φιλοτ]ειμηθείσης καὶ / [ἐπιτελεσ]θείσης [ὑπὸ Αὐρ(ηλίου)] Ζωί/[λου Χρήσ]του τ[ο]ῦ ἀ[ξ]ιολογω/[τάτου] καὶ Αὐρ(ηλίου)] Ζωί/[λου Χρήσ]του τ[ο]ῦ ἀ[ξ]ιολογω/[τάτου] καὶ Αὐρ(ηλίας) [Διοδω]ριανῆς / Θεο]δώρας τῆς γ[υναικ]ὸς [αὐ]/τοῦ τῆς ἀξιολογω[τά]της ἐξ [ἰδί]/ων χρημάτων ἀγωνοθετοῦ […] |
| `Hisp]anor(um)` | EDCS-00000898-0 | (not sub-classified) | [Imp(erator) Caes(ar) divi Hadriani f(ilius) divi Trai/ani Part(hici) nep(os) divi Ne]rva[e pron(epos) T(itus) Ael/ius Hadrianus Anto]ninus Aug(ustus) Pius / [pont(ifex) max(imus), tr(ibunicia) pot(estate) XX], imp(erator) II, co(n)s(ul) IIII, p(ater) p(atriae) / equitib[us) et peditib(us), q]ui mil […] |
| `Iul(io)]` | EDCS-00001360-0 | (not sub-classified) | [Imp(eratore) d(omino) n(ostro) Philippo A]ug(usto) et Ti(ti)ano co(n)s(ulibus) pr(idie) kal(endas) Sept(embres) / Scupin(orum) col(onia) Fl(avia) Vesp(asiana) Aug(usta) Fel(ix) Dard(anorum) Vett(eranorum) praeeun(t)e / L(ucio) Val(erio) Pertinace eq(uite) R(omano) dec(urione) col(oniae) Vim(inacii) […] |
| `pu]bl(icorum)` | EDCS-00400036-0 | (not sub-classified) | [A(ulo) Lar]cio [A(uli) f(ilio) Palatina Crispino(?)] / [3]YLO[3] / [lictori] curiato [3] / promag(istro) II pu]bl(icorum) (XXXXquadragesimae) p[ortuum Asiae |
| `tri]b(unicia)` | EDCS-00900044-0 | (not sub-classified) | ] tri]b(unicia) po[t(estate) 3] / [3]IIV[ |
| `ann(os)]` | EDCS-01000049-0 | (not sub-classified) | ] / r[egnavit] Albae ann(os)] / XXv[ir agr(is) dand(is) a<t=D>tr(ibuendis) iud(icandis)] |
| `qua]est(ori)` | EDCS-01000264-0 | (not sub-classified) | [T(ito) Pomponio T(iti) f(ilio) Pob(lilia?) Proculo] / [Vitrasio Pollioni co(n)s(uli) II ami]/[co Aug]ustorum comiti [M(arci) Antonini] / [et Ver]i Augg(ustorum) expeditio[nis primae] / [Germ]anicae item comiti M(arci) [Antonini] / [et Co]mmodi Augg(ustorum) expediti[onis Germani]/[cae Sar]maticae b […] |
| `m(ensium)]` | EDCS-01000365-1 | (not sub-classified) | Cl(a)udi[an]us f(ilio) b(ene) m(erenti) / Nice mater f(ecit) / filio bene me/renti in [pace] / qui peri[it ann(orum) 3] / m(ensium)] V die[rum 3] |
| `a(nnos)]` | EDCS-02700486-0 | (not sub-classified) | Claudia S]avaria / [3 e]quitum sing(ularium) Aug(usti) / [tur(ma) Sab]iniani(?) nat(ione) / [Pannon]io vix(it) a(nnos)] XXXIIII / [mil(itavit) a(nnos)] XIIII / [3]VFVS[ |
| `Aug(usti)]` | EDCS-02700791-0 | (not sub-classified) | ] Aug(usti)] / lib(ertus?) filio / reverentissimo |
| `f]i(lio)` | EDCS-03000754-0 | (not sub-classified) | Ti(berio) [Cl(audio) Ti(beri) f(ilio) 3 dom]o / T[olentino(?) 3 d]uo/[viro 3]s Sa[3] / Ti(berio) Cl(audio) Ti(beri) f]i(lio) Avono / [et 3]avo |
| `pont(ifici)]` | EDCS-03300043-0 | (not sub-classified) | [Ti(berio) Claudio] Drusi f(ilio) Ca[esari Aug(usto)] / Germanico pont(ifici)] max(imo) tr(ibunicia) pot(estate) V imp(eratori) X [co(n)s(uli) III desig(nato) IV(?) p(atri) p(atriae)] / [3] Honorat[us |
| `Adiab(enici)]` | EDCS-03300819-0 | (not sub-classified) | Imp(erator) Caes(ar) divi M(arci) Anton[ini Pii Germ(anici) Sarm(atici) fil(ius) divi] / Commodi frater divi A[ntonini Pii nep(os) divi Hadr(iani)] / pronep(os) divi Traiani Pa[rthici abnep(os) divi Nervae adnep(os)] / L(ucius) Septimius Severus Pius P[ertin(ax) Aug(ustus) Arab(icus) Adiab(enicus) P […] |
| `m(erito)]` | EDCS-03700690-0 | (not sub-classified) | I(ovi) O(ptimo) M(aximo) / Clau(dius) Can/didinus / mil(es) coh(ortis) / M[auror(um)] / v(otum) s(olvit) l(ibens) m(erito)] |
| `Vel(ina)]` | EDCS-04200052-0 | (not sub-classified) | L(ucius) Aufust[ius 3 f(ilius)] Vel(ina)] / Gallus [IIvir] / quinq(uennalis) [Polae] / ex testam[ento fieri] / [i]ussit a[rbitratu] / Philero[tis l(iberti)] |
| `]p(edes)` | EDCS-04200656-0 | (not sub-classified) | Q(uintus) Ated[ius 3] / Scaev[ola] / v(ivus) [f(ecit)] / Q(uintus) Pos[3] / [3]itra / IO in [f]r(onte) ]p(edes) 3] / in agr(o) [p(edes) |
| `co(n)s(uli)]` | EDCS-04203387-0 | (not sub-classified) | [M(arco) Iuven]tio M(arci) f(ilio) / [Fab(ia) Secun]do Rixae / [Postumio(?) Pan]sae Valerian(o) / [3] Severo / [3 tri]b(uno) leg(ionis) Prim(ae) Min(erviae) / [3 eod]emque tempor(e) / [3 t]ribun(o) pleb(is) pr(aetori) / [3 prae]f(ecto) frument(i) dand(i) / [3]t proco(n)s(uli) provinc(iae) / [3 leg(a […] |
| `ben(e)]` | EDCS-04500017-0 | (not sub-classified) | [Tibi] ben(e)] / [M]arcelo / [It]ali Cae/[r]o conse(rvus) / [vix(it)] an(nos) XVIII / [me]n(ses) VIII / [3]erini |
| `p(assuum)]` | EDCS-04900354-0 | (not sub-classified) | [Liberatori orbis] / [Romani restitutori] / [libertatis ac rei] / [publicae c]ons(ervatori) milit[um]/ [et provi]ncialium [do]/[mino] nostro Ma[gnen]/[tio I]nvicto pr[incipi] / [victo]ri et trium[phatori] / [sempe]r Au[g]usto / m(ilia) p(assuum)] / [ |
| `trib(unicia)]` | EDCS-04900925-1 | (not sub-classified) | [Imp(erator) Caesar divi Nervae f(ilius) Nerva] Traianus / [Aug(ustus) German(icus) Dacic(us) pontif(ex) maxi]m(us) trib(unicia)] / [potest(ate) 3 co(n)s(ul)] V p(ater) p(atriae) [ |
| `dep]os(itus?)` | EDCS-05000027-0 | (not sub-classified) | ] / [du]lcissim[us(?) 3 q(ui?) vix(it)] / [an]nis XXIIII [3] / [d]ie(bu)s XV [3] / dep]os(itus?) V[ |
| `pa]tri(?)` | EDCS-05101012-1 | (not sub-classified) | [P(ublius) Epidius M(arci) f(ilius) Tertu]llus / [VIvir iun(ior)] sibi et / M(arco) Epidio M(arci) f(ilio) Calvo pa]tri(?) / [M(arco) Epidio M(arci) f(ilio) Fronto]ni / [VIvir(o) iun(iori) fratri] / [Petroniae Sex(ti) f(iliae) Maximae mat]ri / [Epidiae M(arci) f(iliae) Paullae sor]ori / [Atiliae Sab […] |
| `m]il(itum)` | EDCS-05200102-0 | (not sub-classified) | Flavoniae L(uci) f(iliae) Pollae / Cordus uxori / M(arcus) Granius M(arci) [f(ilius) M(arci)] n(epos) Cordu[s] trib(unus) m]il(itum) / [praef(ectus) eq(uitum) praef(ectus) fab]r(um) IIvir quinq(uennalis) iter(um) / [aed(ilis) q(uaestor) curat(or) aq]uae du[ce]nd(ae) d(ecreto) d(ecurionum) |
| `Pal(atina)]` | EDCS-05300052-0 | (not sub-classified) | [A(ulo)] L[ar]cio A(uli) f(ilio) Pal(atina)] / Cris[pino] / promag(istro) duum p(ublicorum) XXXX(quadragesimae) p(ortuum) / Asiae et XX(vicesimae) lib(ertatis) pro/vinciarum Asiae / Ponti et Bithyniae / Galatiae Cappadociae / Pisidiae Lycaoniae Pam/phy[l]iae et Ly[cia]e Arme/[niae minoris] / [3]ILIA […] |
| `militarib(us)]` | EDCS-05400230-0 | (not sub-classified) | Q(uinto) Gl[itio P(ubli) f(ilio) Stel(latina)] / Atilio A[gricolae co(n)s(uli) II] / VIIviro ep[ulonum sodali] / Augustali Cl[audiali legat(o) pro pr(aetore)] / Imp(eratoris) Nervae Cae[s(aris) Traian(i) Aug(usti) Ger(manici)] / Dacici provinc[iae Pannoniae] / donato ab eod[em bello Dacico] / donis  […] |
| `lib(erto?)]` | EDCS-05500826-0 | (not sub-classified) | Peculia/ri Cess[e]/ae lib(erto?)] / ann(orum) XL / cul(tores) Lar(um) / pub(licorum) col(legae) / f(aciendum) c(uraverunt) |
| `p]ot(estate)` | EDCS-05501122-0 | (not sub-classified) | p]ro[n(epoti) 3] / trib(unicia) p]ot(estate) 3 co(n)s(uli)] / [3] pro[co(n)s(uli) |
| `co(n)s(uli)]` | EDCS-05501122-0 | (not sub-classified) | p]ro[n(epoti) 3] / trib(unicia) p]ot(estate) 3 co(n)s(uli)] / [3] pro[co(n)s(uli) |
| `m]ax(imo?)` | EDCS-05600211-0 | (not sub-classified) | ]TO[3] / [3]OPO[3] / [3] m]ax(imo?) [3] / [6] / Brac[ara] Aug(usta) / [m(ilia) p(assuum) X]XXV |
| `Sar(matici)]` | EDCS-05601105-0 | (not sub-classified) | [Imp(erator) Caes(ar) M(arcus) Aurelius An]toninus et / [Imp(erator) Caes(ar) L(ucius) Aurelius Commod]us Augg(usti) Ger(manici) Sar(matici)] / [3 m]unicipi(i) Lucent[i(norum)] / [ |
| `f(ecit)]` | EDCS-05800447-0 | (not sub-classified) | D(is) M(anibus) / Aurelio Zo/simo Aure/lia Antioch/is co(n)iugi b(ene) [m(erenti)] / f(ecit)] |
| `sacr(um)]` | EDCS-05801955-0 | (not sub-classified) | Dis [Manibus] sacr(um)] / M(arco) Li[vio Herm]eroti / vestiario de horreis / Agrippinianis / Claudia Ti(beri) f(ilia) Moschis / viro carissimo |
| `coh(ors)]` | EDCS-06100448-0 | (not sub-classified) | [Imp(eratori) Caesari] / [divi Nervae f(ilio)] / [Nervae Traiano] / [Aug(usto)] Ge[r(manico) Dacico] / [Parthi]co pon[t(ifici) max(imo)] / coh(ors)] IIII Th/rac(um) P(ia) F(idelis)] |
| `F(idelis)]` | EDCS-06100448-0 | (not sub-classified) | [Imp(eratori) Caesari] / [divi Nervae f(ilio)] / [Nervae Traiano] / [Aug(usto)] Ge[r(manico) Dacico] / [Parthi]co pon[t(ifici) max(imo)] / coh(ors)] IIII Th/rac(um) P(ia) F(idelis)] |
| `Fel(ici)]` | EDCS-06100463-0 | (not sub-classified) | [I]mp(eratori) [C]aes(ari) [L(ucio)] Sept(imio) Severo P[io Pe]rt[inaci] / [A]ug(usto) [Ar]ab(ico) Adiab(enico) P[ar]t(hico) max(imo) [pontif(ici) max(imo)] / [t]rib(unicia) [po]t(estate) XVI im[p(eratori) XII co(n)s(uli) III proco(n)s(uli) p(atri) p(atriae) et] / Imp(eratori) [Cae]s(ari) M(arco) [A […] |
| `M]unat(ius)` | EDCS-06100623-0 | (not sub-classified) | ]sis Am(maedara?) / [3]edius Fortunat(us) cas(tris) / M(arcus) M]unat(ius) Roman(us) Thagor(a) opt(io) / Aelius Ianuar(ius) Bull(a) / [3]DO[3]T [ |
| `Alex(andrinus)]` | EDCS-08000866-0 | (not sub-classified) | Vadimonium factum / Truphoni Potamonis fil(io) Alex(andrino) / in XIII K(alendas) Apriles primas Romae / in foro Augusto ante statuam / Cn(aei) Senti Saturnini triumpha/[l]em hora quinta HS III(milia) / [d]ari fide rogavit C(aius) Sulpicius / [Cinnamu]s fide promisit / [Trupho Potamoni]s f(ilius) Al […] |
| `L(ucio)]` | EDCS-08000900-3 | (not sub-classified) | [A(ulo?) Vite]lli[o] L(ucio)] Vi(p)s[ta]no Poplicola co(n)[s(ulibus) pr(idie) Non(as) 3] C(aius) Iulius Pru]dens scripsi / [me ro]gasse C(aium) Sulpici[u]m Cin[namum eique] man[d]a[sse uti quantam]/cumq[u]e pecuniam [is] aut E[ros aut [3]us aut Titianus aut Martia]l[i]s ser[vi] eius aut C(aius) Sulp […] |
| `e(st)]` | EDCS-08100315-0 | (not sub-classified) | ] Nice / v(ixit) a(nnos) / LXXXV / h(ic) s(ita) e(st)] |
| `q(uin)q(uennalis)]` | EDCS-08601098-0 | (not sub-classified) | ] q(uin)q(uennalis)] R() R() [ |

This is the category the brief was reaching for in section 1: the abbreviation's own letters are outside the brackets and only its neighbours are restored, as in `Hisp]anor(um)` and `pu]bl(icorum)`. The bracket sits in the same whitespace token but does not cover the letters that matter.

These are the best recovery candidates in the whole audit by quality per unit: the abbreviation is on the stone, the expansion is the editor's normal expansion, and the only contamination is a stray bracket character that can be stripped. The category is small, so the gain is small, but it is clean.

**What recovery would gain.**

| measure | value | share |
| --- | --- | --- |
| pairs recoverable in principle | 339 |  |
| with a usable abbreviation and expansion | 314 | 92.63% |
| distinct abbreviation forms | 162 |  |
| of those, forms absent from the kept set | 7 | 4.32% |
| distinct (abbrev, expansion) types | 229 |  |
| of those, types absent from the kept set | 14 | 6.11% |

Duplication cuts two ways. By **type**, 6.11% of the pair types here are new to the dataset (14 of 229) — a real gain in coverage of rare forms. By **token**, 95.54% of the individual pairs repeat a type the kept set already holds, because the volume sits in the same handful of funerary and imperial formulae. Recovering this category would therefore add a long tail of genuinely new forms while re-weighting the head that is already over-represented.

**Bias check.** How the dropped pairs compare with the kept pairs.

| measure | value | reading |
| --- | --- | --- |
| province distribution (TVD) | 0.322 | a different population |
| century distribution (TVD) | 0.121 | mild skew |
| median inscription length | 263 vs 119 kept | dropped pairs come from longer, more damaged texts |
| mean inscription length | 503 vs 280 kept |  |

Total variation distance: 0.00 means the dropped pairs are spread exactly like the kept ones, 1.00 means they share no common ground. Anything above about 0.15 is a materially different population.

Provinces, dropped share against kept share:

| province | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| Roma | 39 | 11.50% | 22.62% | 0.51 |
| Latium et Campania / Regio I | 33 | 9.73% | 8.79% | 1.11 |
| Pannonia superior | 29 | 8.55% | 2.00% | 4.28 |
| Africa proconsularis | 25 | 7.37% | 8.57% | 0.86 |
| Provincia incerta | 24 | 7.08% | 1.85% | 3.83 |
| Baetica | 14 | 4.13% | 2.25% | 1.83 |
| Raetia | 14 | 4.13% | 0.44% | 9.46 |
| Numidia | 13 | 3.83% | 7.72% | 0.50 |

Centuries, dropped share against kept share (centuries below 0.5% of this category's drops omitted as noise):

| century | dropped | share of dropped | share of kept | lift |
| --- | --- | --- | --- | --- |
| 1AD | 34 | 10.03% | 13.33% | 0.75 |
| 2AD | 83 | 24.48% | 19.84% | 1.23 |
| 3AD | 59 | 17.40% | 9.96% | 1.75 |

Abbreviation frequency profile of the dropped pairs, measured against how often each form survives in the kept set:

| profile | dropped pairs | share |
| --- | --- | --- |
| form never seen in the kept set | 7 | 2.23% |
| form seen fewer than 10 times | 11 | 3.50% |
| form seen 10+ times (already well covered) | 296 | 94.27% |

## 8. `non_alphabetic_expansion` — 27

**Sub-classification.** One label was hiding several situations.

| sub-class | count | share of category |
| --- | --- | --- |
| Greek letters in the expansion | 14 | 51.85% |
| dash placeholder in the expansion | 9 | 33.33% |
| other non-letter character in the expansion | 4 | 14.81% |

**40 examples.**

| raw token | inscription id | sub-class | inscription_text |
| --- | --- | --- | --- |
| `Muc(--)` | EDCS-00000601-0 | dash placeholder in the expansion | Times Va/n(n)onis vic(o?) / Muc(--) vixit / an(n)os / IX |
| `M(άρκου)` | EDCS-00000977-0 | Greek letters in the expansion | [τὸ]ν κτίστην / [ἡ] π[ό]λις δι᾿ ἐπι/[μ]ελητοῦ M(άρκου) / [A]ὐρ(ηλίου) Eὐφράτου / [βο]υλευτοῦ |
| `f(̣ilio)` | EDCS-00001319-0 | other non-letter character in the expansion | [Ausa?]gesio [3]/[3] f(̣ilio) an(norum) XVII[3] / [Ander?]e Hyaheṇ[is f(ilia)] / [ma]ter d(e) s(uo) [f(ecit)] / [h(ic)] s(itus) [e(st)] |
| `tr(íbuf)` | EDCS-08500205-0 | other non-letter character in the expansion | Zoveṣ [3] / tr(íbuf) pl(ífríks) [3] |
| `pl(ífríks)` | EDCS-08500205-0 | other non-letter character in the expansion | Zoveṣ [3] / tr(íbuf) pl(ífríks) [3] |
| `K(alend-)` | EDCS-12300375-1 | dash placeholder in the expansion | [Imp(erator) Caesar divi Nervae f(ilius) Nerva Traianus Optimus Aug(ustus) Germ(anicus) Dacic(us) pontif(ex) maxim(us) trib(unicia) potest(ate) XVIII imp(erator) VIIII co(n)s(ul) VI p(ater) p(atriae) equitibus et peditibus qui militaverunt in alis 3 et cohortibus 3 quae apellantur 3 et sunt in Germa […] |
| `Dec(embr-)` | EDCS-12300375-1 | dash placeholder in the expansion | [Imp(erator) Caesar divi Nervae f(ilius) Nerva Traianus Optimus Aug(ustus) Germ(anicus) Dacic(us) pontif(ex) maxim(us) trib(unicia) potest(ate) XVIII imp(erator) VIIII co(n)s(ul) VI p(ater) p(atriae) equitibus et peditibus qui militaverunt in alis 3 et cohortibus 3 quae apellantur 3 et sunt in Germa […] |
| `co(n)s(ul=)` | EDCS-20600175-0 | other non-letter character in the expansion | ] V co(n)s(ul=) III [ |
| `P(ubli-)` | EDCS-28900346-0 | dash placeholder in the expansion | P(ubli-) Lucr[eti- |
| `Bar(bidi-)` | EDCS-29300476-1 | dash placeholder in the expansion | Philargur(us) hic [cum] Bar(bidi-) Vitale (et) Seia Porci Nice(phori) |
| `M(άρκου)` | EDCS-31300112-0 | Greek letters in the expansion | ἀγαθῇ τύχῃ / [Κ]αλόκαιρος καὶ Εὐτύχ[ης] / [π]ραγματ(ευταὶ) M(άρκου) Αὐρ(ηλίου) Μινδί/[ου] Ματιδιανοῦ Πωλλί/[ω]νος ἀρχώνου μʹ λι[μ]/ένων Ἀσίας καὶ ἐπι/[τ]ρόπου Σεβ(αστοῦ) καὶ βιθυ/νιάρχου δὶς καὶ ἀσιάρ/χου ναῶν τῶν ἐν Ἐφέ/σῳ τὸ τελώνιον κα[ὶ] / σὺν αὐτῷ στοὰν σὺ[ν] / τῷ παντὶ κόσμῳ ἀ(π)[ὸ] / τῶν θεμε […] |
| `XP(ιστος)` | EDCS-32400266-0 | Greek letters in the expansion | A XP(ιστος) Ω |
| `s(it-)` | EDCS-42700223-1 | dash placeholder in the expansion | R[3] / an[n(orum) 3] / ME[3] / h(ic) s(it-) [e(st) s(it) t(ibi) t(erra) l(evis)] / Iu[ |
| `f(ili-)` | EDCS-43300039-0 | dash placeholder in the expansion | ] / Alvei / f(ili-) dat / don/at |
| `v(ivus?-)` | EDCS-64400473-0 | dash placeholder in the expansion | ] / Pra[3] / Coel[3]N[3] / Grat[3] / v(ivus?-) [f(ecit?) |
| `b(δέκα)` | EDCS-64600163-0 | Greek letters in the expansion | Δαμάρχου Ἀχύριος χρυσοι π(έντε) b(δέκα) b(ἑκατόν) |
| `b(ἑκατόν)` | EDCS-64600163-0 | Greek letters in the expansion | Δαμάρχου Ἀχύριος χρυσοι π(έντε) b(δέκα) b(ἑκατόν) |
| `h(εκατόν)` | EDCS-64800172-0 | Greek letters in the expansion | Μ \|(δρακμή) \|(ὀβολός) \|(ὀβολός) \|(ὀβολός) \|(ὀβολός) λεκυθίδες h(εκατόν) Δ(έκα) Δ(έκα) |
| `M(arc-)` | EDCS-65300021-0 | dash placeholder in the expansion | ] / [3] M(arc-) A[3] / [3]SA[3] / [3] Tar[ent |
| `h(εκατόν)` | EDCS-70900657-0 | Greek letters in the expansion | ] / Σώσιππος Σκόπα ἐπρίατο / οἰκίαμ πὰρ Φιλοξένου / Σιλανοῦ καὶ τὰ σκεύεα / τὰ ἐν τᾶι ο[ἰ]κίαι πάντα π(έντε) Τ(άλαντα) / h(εκατόν) h(εκατόν) h(εκατόν) π(έντε) \|(λίτραι?) ἐπʼ ἰερέος Ἐπιδ[3]α / γοπακ[3 Δ]αμαρέ[τ]ου / Ἄσσινος [3]ώτου |
| `h(εκατόν)` | EDCS-70900657-0 | Greek letters in the expansion | ] / Σώσιππος Σκόπα ἐπρίατο / οἰκίαμ πὰρ Φιλοξένου / Σιλανοῦ καὶ τὰ σκεύεα / τὰ ἐν τᾶι ο[ἰ]κίαι πάντα π(έντε) Τ(άλαντα) / h(εκατόν) h(εκατόν) h(εκατόν) π(έντε) \|(λίτραι?) ἐπʼ ἰερέος Ἐπιδ[3]α / γοπακ[3 Δ]αμαρέ[τ]ου / Ἄσσινος [3]ώτου |
| `h(εκατόν)` | EDCS-70900657-0 | Greek letters in the expansion | ] / Σώσιππος Σκόπα ἐπρίατο / οἰκίαμ πὰρ Φιλοξένου / Σιλανοῦ καὶ τὰ σκεύεα / τὰ ἐν τᾶι ο[ἰ]κίαι πάντα π(έντε) Τ(άλαντα) / h(εκατόν) h(εκατόν) h(εκατόν) π(έντε) \|(λίτραι?) ἐπʼ ἰερέος Ἐπιδ[3]α / γοπακ[3 Δ]αμαρέ[τ]ου / Ἄσσινος [3]ώτου |
| `h(ὲξ)` | EDCS-71000451-0 | Greek letters in the expansion | Δαματρίου τρίται hισταμένου ἐπὶ ἀμ(φ)ιπόλου / Παιανίου τοῦ Θεόλλου Δίων Ἀριστάρχου Ἐρι(μεῖος) / δευ(τέρα) δευ(τέρα) ἐπρίατο πὰρ Φιλωνίδα τοῦ Νεμηνίου / Περηκυατα(ίου) πρᾶτα δεκάτα ἔμβασιν hὰν ἐπέβα / ποτὶ Βειδεῖ τῶι Ἀκᾶ τ{υ}ῶι τρίτωι γύα[ι 3]φιται ἡ κα δεῖ λί(τρας) / ἀργυρίου τ(άλαντα) h(ὲξ) h(εκατὸ […] |
| `h(εκατὸν)` | EDCS-71000451-0 | Greek letters in the expansion | Δαματρίου τρίται hισταμένου ἐπὶ ἀμ(φ)ιπόλου / Παιανίου τοῦ Θεόλλου Δίων Ἀριστάρχου Ἐρι(μεῖος) / δευ(τέρα) δευ(τέρα) ἐπρίατο πὰρ Φιλωνίδα τοῦ Νεμηνίου / Περηκυατα(ίου) πρᾶτα δεκάτα ἔμβασιν hὰν ἐπέβα / ποτὶ Βειδεῖ τῶι Ἀκᾶ τ{υ}ῶι τρίτωι γύα[ι 3]φιται ἡ κα δεῖ λί(τρας) / ἀργυρίου τ(άλαντα) h(ὲξ) h(εκατὸ […] |
| `s(eθ)r(e)` | EDCS-71400098-0 | Greek letters in the expansion | s(eθ)r(e) |
| `K(αταχθoνίοις)` | EDCS-81200005-0 | Greek letters in the expansion | Θ(εοῖς) K(αταχθoνίοις) / Σωτᾶς Μαντι/νεὺς πυθικὸς / αὐλητὴς βʹ π[ε]/ρίοδ[ο]ς [ |
| `M(είλια?)` | EDCS-85000010-0 | Greek letters in the expansion | Imp(eratori) Ca[es(ari) L(ucio) Septimio] / Severo Pio [Pertinaci] / Aug(usto) Ara[b(ico) Adiab(enico)] / Parthico et M(arco) A[ur]el[io] / Antonino A[ug(usti) n(ostri) f(ilio)] / [et P(ublio) Sep]/timio Getae [Aug(usti) n(ostri) f(ilio)] / per M[arium Perpetuum leg(atum)] / A[ug(usti) pr(o) pr(aeto […] |

A mixed bag of dashes standing for illegible stretches, combining diacritics and stray Greek letters. Too few to matter and too heterogeneous to rule on mechanically.

## 9. `contains_numeral` — 19

**Sub-classification.** One label was hiding several situations.

| sub-class | count | share of category |
| --- | --- | --- |
| digit inside parentheses = count of lost letters | 11 | 57.89% |
| digit substituted for a letter (transcription typo) | 4 | 21.05% |
| un-decoded HTML entity leaked from the source | 3 | 15.79% |
| other stray digit | 1 | 5.26% |

**40 examples.**

| raw token | inscription id | sub-class | inscription_text |
| --- | --- | --- | --- |
| `Dec(3)` | EDCS-00001435-0 | digit inside parentheses = count of lost letters | Dec(3) |
| `M(3)` | EDCS-09000638-0 | digit inside parentheses = count of lost letters | Q(uintus?) M(3) V(3) |
| `V(3)` | EDCS-09000638-0 | digit inside parentheses = count of lost letters | Q(uintus?) M(3) V(3) |
| `L(3)bo` | EDCS-19100687-0 | digit inside parentheses = count of lost letters | In Ie(su) C(risti) L(3)bo A t S AM CelMeterio |
| `P(huesium)6` | EDCS-23701387-0 | other stray digit | Deo [3]ci[3]ar() / M(arcus) Aemilius Fe/lix m[a]g(ister) <c=K>as/telli Phuen/sium r(es) p(ublica) P(huesium)6 |
| `pu(3)` | EDCS-33300088-0 | digit inside parentheses = count of lost letters | Depositus pu(3) fiect[3] / d(omino) n(ostro) Valentiniano Au<g=C>(usto)[3] / in pace q(ui) vixit ann<o=V>s XX[3] / fecit locum quadrisomum [3] |
| `posteris1(ue)` | EDCS-36300326-0 | digit substituted for a letter (transcription typo) | ]ci / [3] optima / [3 sibi e]t suis liber/[tis libertabusq(ue)] posteris1(ue) / [eorum 3] Flavius |
| `h(a)b(3)to` | EDCS-38600786-0 | digit inside parentheses = count of lost letters | ]vis h(a)b(3)to pudorem / [Ven]erem nolito / [3] opus [3] pure / [ |
| `F(e)br(uar)ii(1)` | EDCS-38700158-0 | digit inside parentheses = count of lost letters | In n(o)m(in)e d(omi)ni cons<e=A>cratum est templum istu(m) a d(o)m(in)o / Gudesteo ep(iscop)o per iussionem d(omi)ni Veremundi principis pro/lis Ordoni in N(ona)s F(e)br(uar)ii(1) XXXIa post m(i)l(le)s(si)ma / sunt ibi reliqui(a)e recondit(a)e id est de L[ |
| `(3)us` | EDCS-52200536-10 | digit inside parentheses = count of lost letters | Mulo Porce[l]/lo m<u=O>lomedico [3] / interficite eum occidite eni[3]/teprofucate Porcellu(m) et Mau/rilla(m) u<xo=SU>re(m) ips[i]us anima cor [1]/nata epar isi[3]e[1]mr / (3)us / [ |
| `ann(0s)` | EDCS-53100212-0 | digit substituted for a letter (transcription typo) | D(is) M(anibus) / Restuta Felicissi/mo co(n)iugi dul/cissimo bene me/renti feci(t) qui vi/xit mecum ann(0s) XX/VIII m(enses) XI |
| `Marc(3)` | EDCS-64500239-0 | digit inside parentheses = count of lost letters | ]orio / [3] vi[x(it)] ann(os) / [3] Licinius / pat(er) posuit mem/or(iam) (A)elia Marc(3) m/at(er) fecer(unt) pat(er) et m/at[e]r [b(ene)] m(erenti) |
| `an(n0s)` | EDCS-75600039-0 | digit substituted for a letter (transcription typo) | D(is) M(anibus) / A(ulo) Ofillio Q(uinti) f(ilio) Fabio / Aquilino Vitali / nomenclatori / Put{a}[eol]is(?) / vixit an(n0s) [3] m(enses) [3] d(ies) [3] / Maria Restituta / infelicissima / [filio(?)] piissimo |
| `p&#x323;(ater)` | EDCS-76600347-0 | un-decoded HTML entity leaked from the source | [Imp(erator) Caes(ar) L(ucius) Septimius] / [Severus Pius Pertinax] Au[g(ustus)] / [Arab(icus) Adiab(enicus) Part(hicus)] max(imus) / [pont(ifex) max(imus) trib(unicia) pot(estate)] VIII im[p(erator)] / X&#x323;I p&#x323;(ater) p(atriae) e&#x323;[t Imp(erator) Caes(ar) M(arcus)] A(u)r(elius) Ant&#x3 […] |
| `ab&#x323;nep(otes)` | EDCS-76700107-0 | un-decoded HTML entity leaked from the source | [Imp(erator) Caesar M(arcus) Aurelius] / [Antoninus II imp(erator) Aug(ustus) ponti]/[fex maximus tribuniciae] / [potestatis XVI co(n)s(ul) III et] / [Imp(erator) Caesar L(ucius) Aurelius] / [Verus Armeniacus Aug(ustus)] / [tribuniciae potestatis II co(n)s(ul) II] / [divi Pii T(iti) Antonini filii]  […] |
| `coh&#x323;(ortem)` | EDCS-76700107-0 | un-decoded HTML entity leaked from the source | [Imp(erator) Caesar M(arcus) Aurelius] / [Antoninus II imp(erator) Aug(ustus) ponti]/[fex maximus tribuniciae] / [potestatis XVI co(n)s(ul) III et] / [Imp(erator) Caesar L(ucius) Aurelius] / [Verus Armeniacus Aug(ustus)] / [tribuniciae potestatis II co(n)s(ul) II] / [divi Pii T(iti) Antonini filii]  […] |
| `MI(3)AGISTRA` | EDCS-81300136-2 | digit inside parentheses = count of lost letters | ]RIT / MI(3)AGISTRA / [3]CITAS PERF/[3 e]t dedit |
| `argent(3)` | EDCS-82400066-0 | digit inside parentheses = count of lost letters | ]++A[3] / [3]+P++A+[3] / uxor / [a]mant() fec(it) / [3]++s argent(3) / [3]+V |
| `d(onavi7)` | EDCS-84900046-0 | digit substituted for a letter (transcription typo) | Tiberius presb<y=I>t(er) servus dei vixit / an(nos) LXXXIV requievit in pace d(omini) Idib(us) / Septembri(s) (a)era DCC[C]XCIII / f(ecit) F(ranciscus) O(liveira) p(resbyter?) E() V() G() H() BI() Q() V() R() H() M() p(osuit?) d(onavi7) M/DCCXLIX |

**Partly confirmed, partly refuted.** The premise is right for the clearest cases: `Dec(3)` does mean three letters are lost after *Dec*, so the parenthesis holds a gap measurement rather than an expansion, and real Roman numerals are carved as letters (`XX`, `III`) and handled elsewhere. But that is not the whole of this category, and with only nineteen tokens every one could be read individually.

Two other things are hiding here, and neither is a numeral:

- **Digit-for-letter transcription typos.** `ann(0s)` is `ann(os)` with a zero for the letter o; `an(n0s)` the same; `d(onavi7)` is `d(onavit)`; `posteris1(ue)` is `posterisq(ue)`. These are correct abbreviations spoiled by a keying slip, and they were dropped for the wrong reason.
- **Un-decoded HTML entities.** `p&#x323;(ater)`, `coh&#x323;(ortem)` and `ab&#x323;nep(otes)` contain a raw `&#x323;` — the character reference for a combining dot below, the epigraphic sign for a partly legible letter. It reached the JSONL unescaped, so the digits are an artifact of the scrape, not of the inscription. This is worth a look upstream: if entities survive un-decoded here they may survive elsewhere in the corpus without tripping any filter.

The exclusion itself is still right — none of these nineteen belong in a training set as they stand — but the reason label is wrong for most of them, and the entity leak is a data-quality signal rather than a parsing decision.

## 10. `nested_parens` — 1

**40 examples.**

| raw token | inscription id | sub-class | inscription_text |
| --- | --- | --- | --- |
| `((mulieris))` | EDCS-00000600-0 | (not sub-classified) | C(aio) Vibuleio ((mulieris)) l(iberto) / Restituto / Aug(ustali) et Claud(iali) / Beneventi / Mnester / lib(ertus) |

A single token, `((mulieris))`, a doubled rendering of the reversed-C sign. One occurrence decides nothing either way.

## Summary

| category | count | recommendation | pairs recoverable | bias risk if kept out |
| --- | --- | --- | --- | --- |
| `inside_bracket_markup` | 253,256 | NEEDS HUMAN REVIEW | 64,991 | high |
| `editorial_marker_paren` | 58,720 | RECOVER AS SEPARATE CLASS | 42,805 | low |
| `non_alphabetic_abbrev` | 16,335 | RECOVER AS SEPARATE CLASS | 16,335 | low |
| `greek_script` | 12,987 | RECOVER AS SEPARATE CLASS | 12,987 | low |
| `unbalanced_parens` | 561 | RECOVER | 338 | low |
| `no_letters_outside_parens` | 469 | KEEP EXCLUDED | 0 | negligible |
| `token_carries_markup` | 339 | RECOVER | 339 | low |
| `non_alphabetic_expansion` | 27 | KEEP EXCLUDED | 0 | negligible |
| `contains_numeral` | 19 | KEEP EXCLUDED | 0 | negligible |
| `nested_parens` | 1 | KEEP EXCLUDED | 0 | negligible |

These do not all add up into one pile, and pooling them would be the same mistake the original filter made in reverse.

| stage | pairs | what it is |
| --- | --- | --- |
| kept today | 1,424,314 | the current dataset |
| + straightforward recoveries | 677 | multi-word expansions and intact abbreviations beside a stray bracket; these are Latin expansion pairs and belong in the main set |
| = Latin expansion set after clean recoveries | 1,424,991 |  |
| + separate labelled classes | 72,127 | abstention cases, symbol abbreviations, Greek — each a different task, none of them Latin letter-to-letter expansion pairs |
| + conditional on a human decision | 64,991 | partly-restored abbreviations; in or out depending on how the task defines its input |
| upper bound if everything is taken | 1,562,109 | not a recommendation |

