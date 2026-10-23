# Abbreviation-expansion feasibility probe

Source: `data/edcs_inscriptions.jsonl`

## 1. Transcription field

The transcription lives in **`inscription_text`**.
Other text-like candidates considered: `['inscription_text']`

### 5 raw examples, verbatim

1. `EDCS-00000001-0`
```
?] C(aius) Trebonius IIvir et praef(ectus) i(ure) d(icundo) civitatis Agunti [?
```
2. `EDCS-00000002-0`
```
]ITSAMARVS[3] / [3]ETS TOVICTO[
```
3. `EDCS-00000003-0`
```
[I(ovi)] O(ptimo) M(aximo)
```
4. `EDCS-00000004-0`
```
L(egio) I M(inervia)
```
5. `EDCS-00000005-0`
```
C(aius) Artilius
```

## 2. Corpus totals

| metric | value | share |
| --- | --- | --- |
| inscriptions scanned | 588,509 |  |
| with a non-empty transcription | 588,349 | 100.0% |
| with >=1 expansion pair | 337,744 | 57.4% |
| total pairs extracted | 1,424,314 |  |
| unique abbreviation forms (case-sensitive) | 40,624 |  |
| unique abbreviation forms (case-folded) | 37,526 |  |
| unique expansions (case-sensitive) | 42,675 |  |
| unique expansions (case-folded) | 40,399 |  |
| unique (abbrev, expansion) pair types | 73,105 |  |

## 3. What was excluded, and why

| reason | count | examples |
| --- | --- | --- |
| inside_bracket_markup | 253,256 | `[I(ovi)]`, `(!)`, `frumen]t(o)`, `[Imp(erator)` |
| editorial_marker_paren | 58,720 | `PR()`, `P()`, `uac(?)`, `M()` |
| non_alphabetic_abbrev | 16,335 | `\|(obitus)`, `Aὐρ(ήλιος)`, `\|(miliaria)`, `\|(crux)` |
| greek_script | 12,987 | `Αύρ(ηλίου)`, `δδ(εοποτῶν)`, `ζ´ἰνδ(ικτιῶος)`, `Κλ(αυδίου)` |
| unbalanced_parens | 561 | `s(itus`, `leg(ionis`, `q(ui`, `Ap(pius` |
| no_letters_outside_parens | 469 | `(filium)`, `(filius)`, `(δηνάρια)`, `(δηνἀρια)` |
| token_carries_markup | 339 | `Αὐρ(ηλίου)]`, `Hisp]anor(um)`, `Iul(io)]`, `pu]bl(icorum)` |
| non_alphabetic_expansion | 27 | `Muc(--)`, `M(άρκου)`, `f(̣ilio)`, `tr(íbuf)` |
| contains_numeral | 19 | `Dec(3)`, `M(3)`, `V(3)`, `L(3)bo` |
| nested_parens | 1 | `((mulieris))` |

## 4. Top 50 abbreviations by frequency

| abbrev | freq | n_expansions | expansions |
| --- | --- | --- | --- |
| m | 137,216 | 501 | manibus (55,028), marcus (15,928), marci (15,502), merenti (8,649), +497 more |
| l | 90,901 | 348 | luci (21,414), lucius (18,241), libertus (10,799), libens (7,711), +344 more |
| d | 87,666 | 399 | dis (54,049), dies (5,592), decreto (2,833), decurionum (2,777), +395 more |
| f | 72,949 | 353 | filius (18,496), fecit (14,905), filio (9,682), filia (5,907), +349 more |
| p | 70,184 | 637 | pedes (10,705), publi (8,813), publius (7,773), patriae (4,028), +633 more |
| s | 66,140 | 609 | sacrum (19,121), situs (12,159), solvit (7,362), sita (7,359), +605 more |
| c | 64,081 | 629 | cai (21,717), caius (19,498), caio (6,665), curavit (2,917), +625 more |
| v | 38,757 | 317 | vixit (17,469), votum (7,925), vivus (1,997), vir (1,497), +313 more |
| a | 30,461 | 419 | annos (16,578), auli (3,144), aulus (2,377), animo (1,250), +415 more |
| aug | 28,162 | 58 | augusti (10,509), augusto (6,269), augustae (3,077), augustus (2,776), +54 more |
| t | 26,435 | 240 | titus (6,296), titi (5,851), tibi (4,973), terra (4,126), +236 more |
| q | 26,169 | 167 | quinti (8,548), quintus (8,081), quinto (2,770), qui (1,663), +163 more |
| h | 25,112 | 158 | hic (20,096), hoc (875), heres (856), heredem (822), +154 more |
| e | 19,095 | 153 | est (17,302), et (330), egregio (190), eius (171), +149 more |
| an | 18,599 | 70 | annos (10,759), annorum (6,352), annis (551), anno (293), +66 more |
| leg | 18,115 | 35 | legio (8,270), legionis (7,043), legato (1,444), legatus (363), +31 more |
| ann | 15,379 | 26 | annos (10,514), annorum (2,900), annis (1,394), anno (175), +22 more |
| n | 14,299 | 295 | nostri (3,471), nostro (1,644), numero (1,450), nummum (1,174), +291 more |
| cos | 14,056 | 48 | consulibus (6,338), consul (2,874), consuli (2,361), consule (861), +44 more |
| vix | 12,731 | 6 | vixit (12,708), vixi (13), vixerunt (7), viximus (1), +2 more |
| imp | 12,641 | 27 | imperator (4,372), imperatori (4,034), imperatoris (2,830), imperatore (1,109), +23 more |
| b | 12,022 | 181 | bene (10,022), bonae (594), bonum (159), bona (132), +177 more |
| pr | 9,769 | 169 | praediis (2,616), praetore (1,418), pro (1,133), primigenia (1,126), +165 more |
| of | 9,002 | 23 | officina (8,936), off (12), officinae (9), officinator (7), +19 more |
| o | 8,420 | 160 | optimo (3,495), ossa (1,312), officina (1,114), oro (1,006), +156 more |
| fil | 7,317 | 17 | filio (2,362), filius (2,122), filiae (1,406), filia (776), +13 more |
| i | 7,206 | 225 | iovi (2,975), iure (1,119), iussit (728), in (402), +221 more |
| lib | 7,032 | 61 | libertus (2,437), liberto (1,124), libertis (890), libertae (720), +57 more |
| caes | 6,339 | 32 | caesari (2,299), caesaris (1,952), caesar (1,491), caesare (321), +28 more |
| ti | 5,786 | 35 | tiberi (2,252), tiberius (2,163), tiberio (1,211), titi (49), +31 more |
| r | 5,764 | 309 | rei (1,226), romanorum (971), res (472), romani (357), +305 more |
| fl | 5,409 | 58 | flavio (1,741), flavius (1,444), flavi (664), flavia (513), +54 more |
| sex | 5,268 | 27 | sexti (2,412), sextus (1,960), sexto (785), sextius (27), +23 more |
| cl | 5,230 | 58 | classis (1,954), claudius (785), claudiae (651), claudi (557), +54 more |
| iul | 4,972 | 21 | iulius (2,177), iuli (749), iulio (675), iulia (443), +17 more |
| aur | 4,878 | 40 | aurelius (2,637), aurelio (1,008), aureli (508), aurelia (343), +36 more |
| cn | 4,801 | 25 | cnaei (2,484), cnaeus (1,494), cnaeo (655), cnaeum (132), +21 more |
| max | 4,733 | 26 | maximus (2,124), maximo (1,808), maximi (676), maximae (48), +22 more |
| mil | 4,712 | 49 | miles (1,650), militavit (928), militum (691), militi (616), +45 more |
| fec | 4,691 | 9 | fecit (4,128), fecerunt (549), feci (5), fecto (3), +5 more |
| coh | 4,659 | 11 | cohortis (2,727), cohors (1,483), cohortibus (204), cohorte (191), +7 more |
| trib | 4,642 | 29 | tribunicia (3,203), tribuno (717), tribunus (390), tribuni (170), +25 more |
| g | 4,426 | 149 | gemina (1,047), gaius (847), gai (553), geminae (503), +145 more |
| kal | 4,198 | 20 | kalendas (3,825), kalendis (295), kalendarum (30), kalendarii (19), +16 more |
| k | 3,492 | 89 | kalendas (2,238), kalendis (439), kastrense (82), kapita (78), +85 more |
| val | 3,201 | 43 | valerius (1,235), valerio (1,114), valeri (340), valeria (177), +39 more |
| ser | 2,887 | 47 | servus (1,241), servi (388), servo (321), sergia (296), +43 more |
| pot | 2,821 | 18 | potestate (2,761), potestatis (35), potaissensis (4), potitus (3), +14 more |
| que | 2,679 | 15 | quae (2,643), quie (14), quem (6), quei (3), +11 more |
| fr | 2,613 | 28 | fronte (2,392), frater (45), fratri (43), fretensis (29), +24 more |

## 5. Ambiguity table

- abbreviations with >1 distinct expansion: **7,836** of 37,526 (20.9%)
- pairs sitting under an ambiguous abbreviation: **1,356,329** of 1,424,314 (95.2%)

Not all ambiguity is the same kind. Most of it is **inflectional** -- the abbreviation stands for one word and only the case ending is in doubt (`co(n)s(ul)` -> consul / consuli / consulibus). The hard and interesting kind is **lexical**, where the same letters stand for genuinely different words (`f` -> filius / fecit / faciendum). Expansions are grouped into words by shared stem, and an abbreviation counts as lexically ambiguous only if its expansions fall into more than one such group.

| class | count | share |
| --- | --- | --- |
| ambiguous, inflectional only | 5,077 | 64.8% |
| ambiguous, genuinely lexical | 2,759 | 35.2% |
| pairs under a lexically ambiguous abbrev | 1,218,090 | 85.5% |

`balance` = Shannon entropy of the expansion distribution normalised by log(n_expansions): 1.0 = a perfectly even split, ~0 = one dominant reading with rare alternatives. Sorted most-balanced first.
Restricted to abbreviations seen >= 20 times, so that a 1-vs-1 split of a hapax does not outrank a genuine coin-flip.

| abbrev | freq | n_expansions | n_words | kind | balance | majority_share | expansions |
| --- | --- | --- | --- | --- | --- | --- | --- |
| adiec | 138 | 2 | 1 | inflectional | 1.0 | 0.5 | adiecti (69), adiectus (69) |
| mense | 35 | 2 | 1 | inflectional | 0.9994 | 0.5143 | mensem (18), menses (17) |
| arabic | 37 | 3 | 1 | inflectional | 0.9954 | 0.3784 | arabico (14), arabici (12), arabicus (11) |
| calpeta | 32 | 2 | 1 | inflectional | 0.9887 | 0.5625 | calpetano (18), calpetani (14) |
| iucu | 40 | 2 | 1 | inflectional | 0.9837 | 0.575 | iucundi (23), iucundus (17) |
| cepiona | 21 | 3 | 2 | lexical | 0.9803 | 0.381 | caepioniana (8), cepioniana (8), caepionianis (5) |
| ianuari | 20 | 4 | 1 | inflectional | 0.9794 | 0.35 | ianuarius (7), ianuario (5), ianuarias (4), ianuaria (4) |
| felicit | 23 | 3 | 1 | inflectional | 0.9783 | 0.4348 | feliciter (10), felicitati (7), felicitas (6) |
| concub | 22 | 2 | 1 | inflectional | 0.976 | 0.5909 | concubinae (13), concubina (9) |
| pla | 20 | 15 | 3 | lexical | 0.9589 | 0.15 | placentia (3), planco (3), plauti (2), placa (1), placido (1), placida (1) |
| militav | 21 | 2 | 2 | lexical | 0.9587 | 0.619 | militavit (13), militaverunt (8) |
| iivr | 26 | 3 | 1 | inflectional | 0.9544 | 0.4231 | iivirum (11), iiviri (10), iivir (5) |
| quisq | 24 | 2 | 1 | inflectional | 0.9544 | 0.625 | quisque (15), quisquis (9) |
| ro | 40 | 22 | 10 | lexical | 0.9537 | 0.1 | romanus (4), romano (4), pro (4), romanorum (3), rogo (2), romani (2) |
| infelicissim | 28 | 5 | 1 | inflectional | 0.9511 | 0.2857 | infelicissimus (8), infelicissimi (7), infelicissima (6), infelicissimo (5), infelicissimae (2) |
| strobil | 31 | 3 | 1 | inflectional | 0.9507 | 0.4516 | strobili (14), strobilus (11), strobilis (6) |
| on | 21 | 15 | 9 | lexical | 0.9504 | 0.1429 | honesta (3), ona (3), onis (3), onita (1), oninianae (1), honoribus (1) |
| comp | 22 | 13 | 3 | lexical | 0.9502 | 0.1818 | comparavit (4), compitali (3), compiti (2), compare (2), compedioni (2), compari (2) |
| aqu | 42 | 21 | 4 | lexical | 0.945 | 0.119 | aquitanorum (5), aquiti (4), aquensium (3), aquarius (3), aquilino (3), aquti (3) |
| xiii | 20 | 9 | 8 | lexical | 0.9414 | 0.2 | xiiimilibus (4), xiiimilia (4), xiiii (3), xiiivictoriarum (2), xiiiquadrantem (2), xiiidodrantem (2) |
| ha | 25 | 15 | 9 | lexical | 0.9402 | 0.16 | hastati (4), hadrumeto (3), habuissent (3), hac (3), haberent (2), hadriani (1) |
| paulin | 30 | 4 | 1 | inflectional | 0.9394 | 0.3333 | paulinus (10), paulini (10), paulinae (7), paulina (3) |
| cin | 32 | 16 | 2 | lexical | 0.9377 | 0.1562 | cinio (5), cinnami (5), cinnamus (3), cini (2), cinis (2), cinereo (2) |
| ven | 65 | 31 | 3 | lexical | 0.933 | 0.1231 | veneri (8), veneris (5), venus (5), venuleio (4), venetiae (3), veneriae (3) |
| britan | 32 | 11 | 1 | inflectional | 0.9325 | 0.2188 | britannica (7), britannico (5), britannicus (4), britannicae (3), britannici (3), britanniae (2) |
| dalmat | 24 | 5 | 1 | inflectional | 0.9313 | 0.375 | dalmatarum (9), dalmatiae (5), dalmata (4), dalmatae (4), dalmatius (2) |
| herme | 21 | 4 | 2 | lexical | 0.9304 | 0.381 | hermes (8), hermetis (7), hermae (3), hermerotis (3) |
| ci | 44 | 24 | 17 | lexical | 0.9298 | 0.1364 | civis (6), cit (5), civitas (4), cicatrices (3), civitatis (3), cius (2) |
| us | 43 | 23 | 19 | lexical | 0.9295 | 0.1395 | vus (6), uxsedi (6), tuus (3), usedi (3), usus (2), bus (2) |
| nostr | 20 | 8 | 1 | inflectional | 0.9281 | 0.2 | nostro (4), nostrorum (4), noster (4), nostrae (3), nostris (2), nostri (1) |
| mel | 23 | 13 | 4 | lexical | 0.9273 | 0.2174 | mellitum (5), melano (3), melissi (3), mellis (2), meli (2), mellariensis (1) |
| libe | 36 | 16 | 1 | inflectional | 0.9263 | 0.1389 | libens (5), libertus (5), libertis (4), liberta (4), liberti (3), libertabusque (3) |
| frument | 27 | 8 | 1 | inflectional | 0.9242 | 0.2593 | frumenti (7), frumentarius (5), frumentariorum (4), frumento (3), frumentariae (3), frumentarii (3) |
| bal | 24 | 13 | 4 | lexical | 0.9238 | 0.1667 | balnei (4), balneis (4), balbino (4), balcaranensi (2), baleriae (2), ballistarium (1) |
| mam | 28 | 14 | 1 | inflectional | 0.9215 | 0.1786 | mamerci (5), mammae (4), mammeianis (4), mama (3), mamercus (2), mamili (2) |
| domne | 20 | 3 | 1 | inflectional | 0.9197 | 0.45 | domnae (9), domine (8), dominae (3) |
| cul | 22 | 14 | 3 | lexical | 0.9191 | 0.2273 | cultores (5), culminali (3), culus (3), culina (1), culario (1), culorum (1) |
| ccc | 33 | 13 | 6 | lexical | 0.9188 | 0.2424 | ccctrecenario (8), cccaiorum (4), cccmilibus (3), ccclarissimi (3), ccclarissimorum (3), cccmilia (2) |
| muna | 36 | 2 | 1 | inflectional | 0.9183 | 0.6667 | munatius (24), munati (12) |
| acil | 21 | 6 | 1 | inflectional | 0.918 | 0.3333 | acili (7), acilius (4), acilia (4), acilio (3), aciliae (2), aciliano (1) |
| capit | 32 | 12 | 1 | inflectional | 0.9173 | 0.2188 | capito (7), capitolio (5), capitali (4), capitolino (3), capitonis (3), capitoniana (2) |
| da | 36 | 19 | 11 | lexical | 0.9171 | 0.1667 | dacia (6), dat (5), daciae (4), das (3), deae (2), data (2) |
| int | 24 | 14 | 4 | lexical | 0.9168 | 0.25 | inter (6), intercalaribus (3), introrsum (2), intro (2), interpres (2), intus (1) |
| ment | 20 | 9 | 3 | lexical | 0.916 | 0.25 | mentis (5), mentum (4), menti (3), mente (2), mento (2), mentorum (1) |
| spl | 20 | 9 | 4 | lexical | 0.916 | 0.25 | splendidissimi (5), splendido (4), splendidissimus (3), splendidissimo (2), splendidissimae (2), sepultus (1) |
| sod | 28 | 4 | 1 | inflectional | 0.9144 | 0.3571 | sodalis (10), sodales (9), sodali (7), sodalium (2) |
| flor | 40 | 17 | 2 | lexical | 0.9143 | 0.15 | florius (6), flori (6), florus (5), florae (4), florentia (3), florentino (2) |
| argent | 38 | 17 | 3 | lexical | 0.9134 | 0.2368 | argentarius (9), argentarii (3), argento (3), argenteam (3), argentum (3), argenteas (2) |
| victori | 21 | 7 | 1 | inflectional | 0.9131 | 0.3333 | victoris (7), victoria (4), victorinus (3), victoriae (2), victorini (2), victorius (2) |
| sarmat | 34 | 3 | 1 | inflectional | 0.9127 | 0.5294 | sarmatici (18), sarmaticus (10), sarmatico (6) |
| alexandr | 27 | 13 | 1 | inflectional | 0.9124 | 0.1852 | alexandriae (5), alexandria (5), alexandri (4), alexandro (2), alexander (2), alexandrianae (2) |
| sin | 59 | 19 | 4 | lexical | 0.9123 | 0.1356 | sine (8), singuli (7), singularis (7), singularium (6), singulari (5), singulas (4) |
| ber | 24 | 13 | 2 | lexical | 0.9123 | 0.25 | berent (6), berytus (3), bertus (3), beri (2), berytensis (2), bergius (1) |
| grat | 36 | 13 | 1 | inflectional | 0.912 | 0.2222 | gratus (8), gratiano (5), gratuito (4), gratias (3), grattius (3), gratuitus (3) |
| negot | 23 | 10 | 1 | inflectional | 0.912 | 0.2174 | negotiatore (5), negotiatori (4), negotiator (4), negotiatoris (3), negotiatores (2), negotiatoribus (1) |
| lustr | 25 | 5 | 2 | lexical | 0.9102 | 0.32 | lustrum (8), lustri (7), lustro (5), lustralis (4), lustraverunt (1) |
| primigen | 42 | 5 | 1 | inflectional | 0.9079 | 0.3571 | primigenius (15), primigeniae (11), primigeni (9), primigenio (4), primigenia (3) |
| tor | 29 | 14 | 5 | lexical | 0.9062 | 0.1724 | tori (5), tuor (5), torum (5), torem (2), torquatae (2), torquibus (2) |
| agripp | 22 | 10 | 1 | inflectional | 0.9049 | 0.2273 | agrippa (5), agrippae (5), agrippinensis (3), agrippinensi (2), agrippinensium (2), agrippiani (1) |
| ves | 31 | 16 | 5 | lexical | 0.9017 | 0.1935 | vespasiano (6), vesti (6), vesta (3), vespasiani (2), vesbinus (2), vespasianus (2) |

_(showing 60 of 1,937 ambiguous abbreviations above the frequency floor)_

## 6. Expansion choice by province and century (top 20 ambiguous)

Does context predict the reading? If an abbreviation's expansion split is flat across every province and century, context carries no signal for it.

### `adiec`  (138 occurrences, 2 expansions)

**by province** (top 12)

| province | adiecti | adiectus | total |
| --- | --- | --- | --- |
| Africa proconsularis | 2 | 53 | 55 |
| Roma | 40 | 0 | 40 |
| Umbria / Regio VI | 8 | 0 | 8 |
| Provincia incerta | 2 | 5 | 7 |
| Sardinia | 6 | 0 | 6 |
| Numidia | 0 | 6 | 6 |
| Mauretania Caesariensis | 0 | 4 | 4 |
| Aemilia / Regio VIII | 3 | 0 | 3 |
| Etruria / Regio VII | 2 | 0 | 2 |
| Latium et Campania / Regio I | 2 | 0 | 2 |
| Hispania citerior | 1 | 0 | 1 |
| Venetia et Histria / Regio X | 1 | 0 | 1 |

**by century** (midpoint of the dating range)

| century | adiecti | adiectus | total |
| --- | --- | --- | --- |
| 1AD | 0 | 1 | 1 |
| 2AD | 66 | 3 | 69 |

### `mense`  (35 occurrences, 2 expansions)

**by province** (top 12)

| province | mensem | menses | total |
| --- | --- | --- | --- |
| Roma | 8 | 8 | 16 |
| Africa proconsularis | 5 | 3 | 8 |
| Lusitania | 2 | 0 | 2 |
| Mauretania Caesariensis | 1 | 1 | 2 |
| Latium et Campania / Regio I | 0 | 2 | 2 |
| Transpadana / Regio XI | 1 | 0 | 1 |
| Pannonia inferior | 1 | 0 | 1 |
| Apulia et Calabria / Regio II | 0 | 1 | 1 |
| Bruttium et Lucania / Regio III | 0 | 1 | 1 |
| Moesia inferior | 0 | 1 | 1 |

**by century** (midpoint of the dating range)

| century | mensem | menses | total |
| --- | --- | --- | --- |
| 1AD | 0 | 1 | 1 |
| 2AD | 0 | 2 | 2 |
| 3AD | 0 | 1 | 1 |
| 4AD | 9 | 5 | 14 |
| 5AD | 1 | 1 | 2 |

### `arabic`  (37 occurrences, 3 expansions)

**by province** (top 12)

| province | arabico | arabici | arabicus | total |
| --- | --- | --- | --- | --- |
| Latium et Campania / Regio I | 2 | 0 | 4 | 6 |
| Numidia | 2 | 4 | 0 | 6 |
| Roma | 2 | 2 | 2 | 6 |
| Africa proconsularis | 2 | 3 | 0 | 5 |
| Raetia | 1 | 0 | 3 | 4 |
| Umbria / Regio VI | 2 | 0 | 0 | 2 |
| Pannonia superior | 2 | 0 | 0 | 2 |
| Provincia incerta | 0 | 1 | 1 | 2 |
| Aegyptus | 1 | 0 | 0 | 1 |
| Apulia et Calabria / Regio II | 0 | 1 | 0 | 1 |
| Syria | 0 | 1 | 0 | 1 |
| Moesia inferior | 0 | 0 | 1 | 1 |

**by century** (midpoint of the dating range)

| century | arabico | arabici | arabicus | total |
| --- | --- | --- | --- | --- |
| 2AD | 4 | 3 | 2 | 9 |
| 3AD | 8 | 3 | 7 | 18 |

### `calpeta`  (32 occurrences, 2 expansions)

**by province** (top 12)

| province | calpetano | calpetani | total |
| --- | --- | --- | --- |
| Roma | 11 | 11 | 22 |
| Provincia incerta | 3 | 0 | 3 |
| Latium et Campania / Regio I | 1 | 2 | 3 |
| Aemilia / Regio VIII | 2 | 0 | 2 |
| Picenum / Regio V | 1 | 0 | 1 |
| Aquitania / Aquitanica | 0 | 1 | 1 |

### `iucu`  (40 occurrences, 2 expansions)

**by province** (top 12)

| province | iucundi | iucundus | total |
| --- | --- | --- | --- |
| Germania inferior | 3 | 6 | 9 |
| Aquitania / Aquitanica | 6 | 2 | 8 |
| Gallia Narbonensis | 2 | 4 | 6 |
| Belgica | 5 | 0 | 5 |
| Hispania citerior | 2 | 0 | 2 |
| Germania superior | 1 | 1 | 2 |
| Etruria / Regio VII | 0 | 2 | 2 |
| Lusitania | 1 | 0 | 1 |
| Mauretania Tingitana | 1 | 0 | 1 |
| Africa proconsularis | 1 | 0 | 1 |
| Roma | 1 | 0 | 1 |
| Umbria / Regio VI | 0 | 1 | 1 |

### `cepiona`  (21 occurrences, 3 expansions)

**by province** (top 12)

| province | caepioniana | cepioniana | caepionianis | total |
| --- | --- | --- | --- | --- |
| Roma | 6 | 8 | 5 | 19 |
| Provincia incerta | 2 | 0 | 0 | 2 |

**by century** (midpoint of the dating range)

| century | caepioniana | cepioniana | caepionianis | total |
| --- | --- | --- | --- | --- |
| 2AD | 7 | 8 | 4 | 19 |

### `ianuari`  (20 occurrences, 4 expansions)

**by province** (top 12)

| province | ianuarius | ianuario | ianuarias | ianuaria | total |
| --- | --- | --- | --- | --- | --- |
| Roma | 2 | 2 | 1 | 0 | 5 |
| Latium et Campania / Regio I | 1 | 1 | 1 | 2 | 5 |
| Africa proconsularis | 1 | 0 | 1 | 1 | 3 |
| Germania inferior | 1 | 0 | 0 | 0 | 1 |
| Mauretania Caesariensis | 1 | 0 | 0 | 0 | 1 |
| Etruria / Regio VII | 1 | 0 | 0 | 0 | 1 |
| Dalmatia | 0 | 1 | 0 | 0 | 1 |
| Pannonia superior | 0 | 1 | 0 | 0 | 1 |
| Hispania citerior | 0 | 0 | 1 | 0 | 1 |
| Umbria / Regio VI | 0 | 0 | 0 | 1 | 1 |

**by century** (midpoint of the dating range)

| century | ianuarius | ianuario | ianuarias | ianuaria | total |
| --- | --- | --- | --- | --- | --- |
| 1AD | 0 | 1 | 0 | 0 | 1 |
| 2AD | 0 | 1 | 0 | 2 | 3 |
| 3AD | 2 | 1 | 1 | 1 | 5 |
| 5AD | 0 | 0 | 1 | 0 | 1 |

### `felicit`  (23 occurrences, 3 expansions)

**by province** (top 12)

| province | feliciter | felicitati | felicitas | total |
| --- | --- | --- | --- | --- |
| Latium et Campania / Regio I | 1 | 3 | 1 | 5 |
| Roma | 0 | 3 | 1 | 4 |
| Africa proconsularis | 2 | 0 | 1 | 3 |
| Transpadana / Regio XI | 1 | 0 | 1 | 2 |
| Numidia | 1 | 0 | 0 | 1 |
| Picenum / Regio V | 1 | 0 | 0 | 1 |
| Aegyptus | 1 | 0 | 0 | 1 |
| Gallia Narbonensis | 1 | 0 | 0 | 1 |
| Aquitania / Aquitanica | 1 | 0 | 0 | 1 |
| Arabia | 1 | 0 | 0 | 1 |
| Mauretania Tingitana | 0 | 1 | 0 | 1 |
| Umbria / Regio VI | 0 | 0 | 1 | 1 |

**by century** (midpoint of the dating range)

| century | feliciter | felicitati | felicitas | total |
| --- | --- | --- | --- | --- |
| 1AD | 1 | 1 | 0 | 2 |
| 2AD | 0 | 2 | 1 | 3 |
| 3AD | 2 | 1 | 1 | 4 |
| 4AD | 0 | 0 | 1 | 1 |
| 6AD | 1 | 0 | 0 | 1 |

### `concub`  (22 occurrences, 2 expansions)

**by province** (top 12)

| province | concubinae | concubina | total |
| --- | --- | --- | --- |
| Roma | 4 | 6 | 10 |
| Latium et Campania / Regio I | 3 | 2 | 5 |
| Aemilia / Regio VIII | 3 | 0 | 3 |
| Picenum / Regio V | 1 | 1 | 2 |
| Venetia et Histria / Regio X | 1 | 0 | 1 |
| Samnium / Regio IV | 1 | 0 | 1 |

**by century** (midpoint of the dating range)

| century | concubinae | concubina | total |
| --- | --- | --- | --- |
| 1BC | 2 | 1 | 3 |
| 1AD | 8 | 5 | 13 |
| 2AD | 0 | 1 | 1 |

### `pla`  (20 occurrences, 15 expansions)

**by province** (top 12)

| province | placentia | planco | plauti | placa | placido | total |
| --- | --- | --- | --- | --- | --- | --- |
| Germania superior | 2 | 0 | 0 | 0 | 0 | 2 |
| Provincia incerta | 0 | 2 | 0 | 0 | 0 | 2 |
| Venetia et Histria / Regio X | 0 | 0 | 2 | 0 | 0 | 2 |
| Pannonia superior | 1 | 0 | 0 | 0 | 0 | 1 |
| Roma | 0 | 1 | 0 | 0 | 0 | 1 |
| Transpadana / Regio XI | 0 | 0 | 0 | 1 | 0 | 1 |
| Liguria / Regio IX | 0 | 0 | 0 | 0 | 1 | 1 |

**by century** (midpoint of the dating range)

| century | placentia | planco | placa | placido | total |
| --- | --- | --- | --- | --- | --- |
| 1BC | 0 | 3 | 0 | 0 | 3 |
| 1AD | 3 | 0 | 0 | 0 | 3 |
| 2AD | 0 | 0 | 1 | 0 | 1 |
| 5AD | 0 | 0 | 0 | 1 | 1 |

### `militav`  (21 occurrences, 2 expansions)

**by province** (top 12)

| province | militavit | militaverunt | total |
| --- | --- | --- | --- |
| Roma | 5 | 0 | 5 |
| Provincia incerta | 0 | 5 | 5 |
| Moesia inferior | 1 | 2 | 3 |
| Umbria / Regio VI | 1 | 0 | 1 |
| Africa proconsularis | 1 | 0 | 1 |
| Etruria / Regio VII | 1 | 0 | 1 |
| Macedonia | 1 | 0 | 1 |
| Dalmatia | 1 | 0 | 1 |
| Hispania citerior | 1 | 0 | 1 |
| Syria | 1 | 0 | 1 |
| Palaestina | 0 | 1 | 1 |

**by century** (midpoint of the dating range)

| century | militavit | militaverunt | total |
| --- | --- | --- | --- |
| 1AD | 3 | 0 | 3 |
| 2AD | 6 | 5 | 11 |
| 3AD | 3 | 0 | 3 |

### `iivr`  (26 occurrences, 3 expansions)

**by province** (top 12)

| province | iivirum | iiviri | iivir | total |
| --- | --- | --- | --- | --- |
| Latium et Campania / Regio I | 11 | 0 | 1 | 12 |
| Britannia | 0 | 10 | 0 | 10 |
| Germania inferior | 0 | 0 | 1 | 1 |
| Moesia superior | 0 | 0 | 1 | 1 |
| Africa proconsularis | 0 | 0 | 1 | 1 |
| Asia | 0 | 0 | 1 | 1 |

**by century** (midpoint of the dating range)

| century | iivirum | iivir | total |
| --- | --- | --- | --- |
| 1AD | 6 | 0 | 6 |
| 2AD | 0 | 2 | 2 |

### `quisq`  (24 occurrences, 2 expansions)

**by province** (top 12)

| province | quisque | quisquis | total |
| --- | --- | --- | --- |
| Roma | 6 | 3 | 9 |
| Baetica | 3 | 0 | 3 |
| Latium et Campania / Regio I | 2 | 1 | 3 |
| Numidia | 1 | 1 | 2 |
| Lusitania | 1 | 1 | 2 |
| Dalmatia | 2 | 0 | 2 |
| Pontus et Bithynia | 0 | 1 | 1 |
| Liguria / Regio IX | 0 | 1 | 1 |
| Africa proconsularis | 0 | 1 | 1 |

**by century** (midpoint of the dating range)

| century | quisque | quisquis | total |
| --- | --- | --- | --- |
| 1AD | 1 | 0 | 1 |
| 2AD | 4 | 4 | 8 |
| 3AD | 1 | 0 | 1 |
| 4AD | 2 | 0 | 2 |
| 5AD | 0 | 1 | 1 |
| 6AD | 1 | 0 | 1 |
| 7AD | 1 | 0 | 1 |

### `ro`  (40 occurrences, 22 expansions)

**by province** (top 12)

| province | romanus | romano | pro | romanorum | rogo | total |
| --- | --- | --- | --- | --- | --- | --- |
| Africa proconsularis | 2 | 3 | 1 | 0 | 0 | 6 |
| Dalmatia | 1 | 0 | 1 | 0 | 0 | 2 |
| Latium et Campania / Regio I | 1 | 0 | 1 | 0 | 0 | 2 |
| Roma | 0 | 0 | 0 | 1 | 1 | 2 |
| Mauretania Caesariensis | 0 | 1 | 0 | 0 | 0 | 1 |
| Numidia | 0 | 0 | 1 | 0 | 0 | 1 |
| Dacia | 0 | 0 | 0 | 1 | 0 | 1 |
| Pannonia inferior | 0 | 0 | 0 | 1 | 0 | 1 |
| Hispania citerior | 0 | 0 | 0 | 0 | 1 | 1 |

**by century** (midpoint of the dating range)

| century | romanus | pro | romanorum | rogo | total |
| --- | --- | --- | --- | --- | --- |
| 1AD | 1 | 0 | 0 | 0 | 1 |
| 2AD | 0 | 1 | 2 | 1 | 4 |
| 3AD | 1 | 1 | 0 | 0 | 2 |
| 5AD | 0 | 1 | 0 | 0 | 1 |

### `infelicissim`  (28 occurrences, 5 expansions)

**by province** (top 12)

| province | infelicissimus | infelicissimi | infelicissima | infelicissimo | infelicissimae | total |
| --- | --- | --- | --- | --- | --- | --- |
| Roma | 4 | 4 | 2 | 1 | 0 | 11 |
| Samnium / Regio IV | 2 | 1 | 0 | 1 | 0 | 4 |
| Venetia et Histria / Regio X | 1 | 0 | 0 | 1 | 0 | 2 |
| Latium et Campania / Regio I | 0 | 1 | 1 | 0 | 0 | 2 |
| Aemilia / Regio VIII | 0 | 0 | 2 | 0 | 0 | 2 |
| Dalmatia | 0 | 0 | 0 | 0 | 2 | 2 |
| Hispania citerior | 1 | 0 | 0 | 0 | 0 | 1 |
| Gallia Narbonensis | 0 | 1 | 0 | 0 | 0 | 1 |
| Baetica | 0 | 0 | 1 | 0 | 0 | 1 |
| Lugudunensis | 0 | 0 | 0 | 1 | 0 | 1 |
| Macedonia | 0 | 0 | 0 | 1 | 0 | 1 |

**by century** (midpoint of the dating range)

| century | infelicissimus | infelicissimi | infelicissima | infelicissimo | infelicissimae | total |
| --- | --- | --- | --- | --- | --- | --- |
| 1AD | 0 | 0 | 0 | 1 | 0 | 1 |
| 2AD | 2 | 3 | 3 | 1 | 0 | 9 |
| 3AD | 0 | 0 | 0 | 0 | 1 | 1 |
| 4AD | 0 | 1 | 0 | 0 | 0 | 1 |

### `strobil`  (31 occurrences, 3 expansions)

**by province** (top 12)

| province | strobili | strobilus | strobilis | total |
| --- | --- | --- | --- | --- |
| Gallia Narbonensis | 5 | 6 | 4 | 15 |
| Aemilia / Regio VIII | 3 | 0 | 1 | 4 |
| Germania inferior | 2 | 1 | 0 | 3 |
| Provincia incerta | 1 | 1 | 1 | 3 |
| Belgica | 1 | 1 | 0 | 2 |
| Dalmatia | 0 | 2 | 0 | 2 |
| Latium et Campania / Regio I | 1 | 0 | 0 | 1 |
| Germania superior | 1 | 0 | 0 | 1 |

### `on`  (21 occurrences, 15 expansions)

**by province** (top 12)

| province | honesta | ona | onis | onita | oninianae | total |
| --- | --- | --- | --- | --- | --- | --- |
| Roma | 2 | 0 | 0 | 0 | 0 | 2 |
| Africa proconsularis | 0 | 1 | 1 | 0 | 0 | 2 |
| Raetia | 1 | 0 | 0 | 0 | 0 | 1 |
| Germania superior | 0 | 1 | 0 | 0 | 0 | 1 |
| Mauretania Caesariensis | 0 | 1 | 0 | 0 | 0 | 1 |
| Apulia et Calabria / Regio II | 0 | 0 | 1 | 0 | 0 | 1 |
| Lusitania | 0 | 0 | 1 | 0 | 0 | 1 |
| Aquitania / Aquitanica | 0 | 0 | 0 | 1 | 0 | 1 |
| Pannonia superior | 0 | 0 | 0 | 0 | 1 | 1 |

**by century** (midpoint of the dating range)

| century | honesta | ona | onis | onita | oninianae | total |
| --- | --- | --- | --- | --- | --- | --- |
| 1AD | 0 | 1 | 0 | 0 | 0 | 1 |
| 2AD | 1 | 1 | 1 | 1 | 0 | 4 |
| 3AD | 1 | 0 | 0 | 0 | 1 | 2 |

### `comp`  (22 occurrences, 13 expansions)

**by province** (top 12)

| province | comparavit | compitali | compiti | compare | compedioni | total |
| --- | --- | --- | --- | --- | --- | --- |
| Roma | 2 | 0 | 1 | 2 | 0 | 5 |
| Umbria / Regio VI | 0 | 2 | 0 | 0 | 0 | 2 |
| Hispania citerior | 0 | 0 | 0 | 0 | 2 | 2 |
| Transpadana / Regio XI | 1 | 0 | 0 | 0 | 0 | 1 |
| Apulia et Calabria / Regio II | 1 | 0 | 0 | 0 | 0 | 1 |
| Samnium / Regio IV | 0 | 1 | 0 | 0 | 0 | 1 |
| Etruria / Regio VII | 0 | 0 | 1 | 0 | 0 | 1 |

**by century** (midpoint of the dating range)

| century | comparavit | compitali | compiti | compare | total |
| --- | --- | --- | --- | --- | --- |
| 1BC | 0 | 2 | 0 | 0 | 2 |
| 1AD | 1 | 0 | 2 | 0 | 3 |
| 4AD | 0 | 0 | 0 | 2 | 2 |
| 5AD | 1 | 0 | 0 | 0 | 1 |

### `aqu`  (42 occurrences, 21 expansions)

**by province** (top 12)

| province | aquitanorum | aquiti | aquensium | aquarius | aquilino | total |
| --- | --- | --- | --- | --- | --- | --- |
| Germania superior | 4 | 0 | 1 | 0 | 0 | 5 |
| Aquitania / Aquitanica | 0 | 4 | 0 | 0 | 0 | 4 |
| Gallia Narbonensis | 0 | 0 | 2 | 1 | 0 | 3 |
| Roma | 0 | 0 | 0 | 0 | 3 | 3 |
| Dalmatia | 1 | 0 | 0 | 0 | 0 | 1 |
| Macedonia | 0 | 0 | 0 | 1 | 0 | 1 |
| Etruria / Regio VII | 0 | 0 | 0 | 1 | 0 | 1 |

**by century** (midpoint of the dating range)

| century | aquitanorum | aquensium | aquarius | aquilino | total |
| --- | --- | --- | --- | --- | --- |
| 1AD | 1 | 0 | 0 | 0 | 1 |
| 2AD | 4 | 1 | 1 | 1 | 7 |

### `xiii`  (20 occurrences, 9 expansions)

**by province** (top 12)

| province | xiiimilibus | xiiimilia | xiiii | xiiivictoriarum | xiiiquadrantem | total |
| --- | --- | --- | --- | --- | --- | --- |
| Aemilia / Regio VIII | 3 | 2 | 0 | 0 | 0 | 5 |
| Pannonia superior | 0 | 0 | 2 | 0 | 0 | 2 |
| Latium et Campania / Regio I | 0 | 0 | 0 | 2 | 0 | 2 |
| Hispania citerior | 0 | 0 | 0 | 0 | 2 | 2 |
| Numidia | 1 | 0 | 0 | 0 | 0 | 1 |
| Dalmatia | 0 | 1 | 0 | 0 | 0 | 1 |
| Venetia et Histria / Regio X | 0 | 1 | 0 | 0 | 0 | 1 |
| Germania superior | 0 | 0 | 1 | 0 | 0 | 1 |

**by century** (midpoint of the dating range)

| century | xiiimilia | xiiii | total |
| --- | --- | --- | --- |
| 1AD | 1 | 0 | 1 |
| 2AD | 1 | 1 | 2 |

## 6b. The same breakdown for the highest-volume ambiguous abbreviations

Ranking by balance alone puts rare, evenly-split forms on top. These are the abbreviations that actually carry the corpus's weight, and they are where a disambiguation model would win or lose.

### `m`  (137,216 occurrences, 501 expansions)

**by province** (top 12)

| province | manibus | marcus | marci | merenti | marco | total |
| --- | --- | --- | --- | --- | --- | --- |
| Roma | 14,459 | 4,986 | 4,291 | 4,082 | 1,668 | 29,486 |
| Latium et Campania / Regio I | 4,355 | 1,992 | 2,599 | 1,222 | 965 | 11,133 |
| Africa proconsularis | 8,394 | 1,092 | 684 | 30 | 370 | 10,570 |
| Numidia | 7,986 | 1,239 | 695 | 134 | 378 | 10,432 |
| Gallia Narbonensis | 1,735 | 369 | 458 | 13 | 132 | 2,707 |
| Dalmatia | 1,629 | 205 | 147 | 535 | 143 | 2,659 |
| Venetia et Histria / Regio X | 702 | 544 | 882 | 107 | 293 | 2,528 |
| Hispania citerior | 1,389 | 324 | 419 | 113 | 277 | 2,522 |
| Mauretania Caesariensis | 1,737 | 260 | 167 | 55 | 128 | 2,347 |
| Apulia et Calabria / Regio II | 955 | 286 | 300 | 511 | 137 | 2,189 |
| Etruria / Regio VII | 673 | 245 | 510 | 239 | 110 | 1,777 |
| Samnium / Regio IV | 682 | 222 | 337 | 241 | 169 | 1,651 |

**by century** (midpoint of the dating range)

| century | manibus | marcus | marci | merenti | marco | total |
| --- | --- | --- | --- | --- | --- | --- |
| 4BC | 0 | 0 | 1 | 0 | 0 | 1 |
| 3BC | 0 | 32 | 54 | 0 | 2 | 88 |
| 2BC | 0 | 98 | 109 | 0 | 6 | 213 |
| 1BC | 11 | 785 | 1,306 | 8 | 240 | 2,350 |
| 1AD | 2,918 | 2,439 | 3,002 | 825 | 1,075 | 10,259 |
| 2AD | 16,634 | 2,835 | 1,822 | 3,418 | 1,627 | 26,336 |
| 3AD | 5,369 | 1,660 | 921 | 1,342 | 1,434 | 10,726 |
| 4AD | 449 | 63 | 21 | 360 | 90 | 983 |
| 5AD | 79 | 4 | 1 | 24 | 1 | 109 |
| 6AD | 20 | 1 | 0 | 5 | 0 | 26 |
| 7AD | 7 | 0 | 0 | 0 | 0 | 7 |
| 17AD | 1 | 0 | 0 | 0 | 0 | 1 |
| 18AD | 1 | 0 | 0 | 0 | 0 | 1 |

### `l`  (90,901 occurrences, 348 expansions)

**by province** (top 12)

| province | luci | lucius | libertus | libens | lucio | total |
| --- | --- | --- | --- | --- | --- | --- |
| Roma | 5,644 | 4,899 | 5,221 | 104 | 1,812 | 17,680 |
| Latium et Campania / Regio I | 2,786 | 2,121 | 1,710 | 73 | 1,038 | 7,728 |
| Venetia et Histria / Regio X | 1,461 | 908 | 647 | 441 | 433 | 3,890 |
| Africa proconsularis | 721 | 1,466 | 43 | 475 | 258 | 2,963 |
| Numidia | 706 | 1,470 | 15 | 182 | 198 | 2,571 |
| Gallia Narbonensis | 987 | 583 | 248 | 444 | 236 | 2,498 |
| Etruria / Regio VII | 1,168 | 575 | 336 | 63 | 185 | 2,327 |
| Hispania citerior | 692 | 542 | 133 | 472 | 345 | 2,184 |
| Samnium / Regio IV | 681 | 461 | 529 | 65 | 229 | 1,965 |
| Umbria / Regio VI | 617 | 372 | 328 | 27 | 174 | 1,518 |
| Germania superior | 300 | 269 | 24 | 588 | 47 | 1,228 |
| Apulia et Calabria / Regio II | 425 | 332 | 270 | 26 | 168 | 1,221 |

**by century** (midpoint of the dating range)

| century | luci | lucius | libertus | libens | lucio | total |
| --- | --- | --- | --- | --- | --- | --- |
| 4BC | 1 | 2 | 0 | 0 | 0 | 3 |
| 3BC | 57 | 64 | 23 | 1 | 2 | 147 |
| 2BC | 160 | 125 | 86 | 6 | 32 | 409 |
| 1BC | 1,961 | 1,181 | 2,015 | 79 | 306 | 5,542 |
| 1AD | 4,376 | 3,642 | 4,064 | 785 | 1,668 | 14,535 |
| 2AD | 2,076 | 2,868 | 476 | 2,592 | 1,731 | 9,743 |
| 3AD | 551 | 783 | 38 | 1,298 | 502 | 3,172 |
| 4AD | 16 | 49 | 5 | 14 | 52 | 136 |
| 5AD | 0 | 4 | 0 | 0 | 0 | 4 |
| 6AD | 2 | 0 | 0 | 1 | 0 | 3 |
| 7AD | 1 | 0 | 0 | 0 | 0 | 1 |

### `d`  (87,666 occurrences, 399 expansions)

**by province** (top 12)

| province | dis | dies | decreto | decurionum | de | total |
| --- | --- | --- | --- | --- | --- | --- |
| Roma | 14,303 | 3,278 | 35 | 35 | 350 | 18,001 |
| Africa proconsularis | 8,104 | 420 | 533 | 519 | 95 | 9,671 |
| Numidia | 7,797 | 75 | 256 | 257 | 40 | 8,425 |
| Latium et Campania / Regio I | 4,325 | 617 | 402 | 407 | 197 | 5,948 |
| Gallia Narbonensis | 1,716 | 55 | 50 | 45 | 70 | 1,936 |
| Dalmatia | 1,631 | 59 | 84 | 79 | 23 | 1,876 |
| Mauretania Caesariensis | 1,693 | 103 | 24 | 24 | 18 | 1,862 |
| Hispania citerior | 1,346 | 36 | 129 | 119 | 101 | 1,731 |
| Apulia et Calabria / Regio II | 941 | 104 | 112 | 119 | 33 | 1,309 |
| Baetica | 847 | 20 | 142 | 148 | 149 | 1,306 |
| Lusitania | 970 | 18 | 34 | 35 | 138 | 1,195 |
| Venetia et Histria / Regio X | 696 | 132 | 136 | 143 | 51 | 1,158 |

**by century** (midpoint of the dating range)

| century | dis | dies | decreto | decurionum | de | total |
| --- | --- | --- | --- | --- | --- | --- |
| 3BC | 0 | 0 | 0 | 0 | 1 | 1 |
| 2BC | 0 | 0 | 1 | 1 | 11 | 13 |
| 1BC | 11 | 6 | 111 | 126 | 165 | 419 |
| 1AD | 2,859 | 445 | 444 | 445 | 369 | 4,562 |
| 2AD | 16,443 | 1,245 | 846 | 798 | 366 | 19,698 |
| 3AD | 5,396 | 457 | 330 | 324 | 58 | 6,565 |
| 4AD | 454 | 1,240 | 24 | 20 | 18 | 1,756 |
| 5AD | 82 | 164 | 0 | 0 | 2 | 248 |
| 6AD | 21 | 31 | 0 | 0 | 2 | 54 |
| 7AD | 7 | 4 | 0 | 0 | 1 | 12 |
| 9AD | 0 | 1 | 0 | 0 | 0 | 1 |
| 17AD | 1 | 0 | 0 | 0 | 0 | 1 |
| 18AD | 1 | 0 | 0 | 0 | 0 | 1 |

### `f`  (72,949 occurrences, 353 expansions)

**by province** (top 12)

| province | filius | fecit | filio | filia | filiae | total |
| --- | --- | --- | --- | --- | --- | --- |
| Roma | 3,351 | 2,182 | 1,136 | 669 | 1,003 | 8,341 |
| Latium et Campania / Regio I | 1,768 | 809 | 1,134 | 463 | 555 | 4,729 |
| Venetia et Histria / Regio X | 1,339 | 865 | 808 | 469 | 558 | 4,039 |
| Numidia | 1,101 | 251 | 121 | 694 | 58 | 2,225 |
| Belgica | 29 | 2,156 | 16 | 9 | 5 | 2,215 |
| Hispania citerior | 680 | 221 | 658 | 315 | 261 | 2,135 |
| Samnium / Regio IV | 926 | 157 | 498 | 265 | 192 | 2,038 |
| Lusitania | 898 | 114 | 260 | 583 | 175 | 2,030 |
| Africa proconsularis | 993 | 243 | 276 | 398 | 75 | 1,985 |
| Gallia Narbonensis | 449 | 669 | 342 | 192 | 286 | 1,938 |
| Germania superior | 313 | 1,384 | 52 | 19 | 10 | 1,778 |
| Etruria / Regio VII | 792 | 174 | 247 | 222 | 104 | 1,539 |

**by century** (midpoint of the dating range)

| century | filius | fecit | filio | filia | filiae | total |
| --- | --- | --- | --- | --- | --- | --- |
| 4BC | 2 | 0 | 0 | 0 | 0 | 2 |
| 3BC | 143 | 0 | 2 | 26 | 3 | 174 |
| 2BC | 479 | 1 | 14 | 64 | 5 | 563 |
| 1BC | 2,627 | 94 | 632 | 590 | 210 | 4,153 |
| 1AD | 5,098 | 1,694 | 3,037 | 1,649 | 1,538 | 13,016 |
| 2AD | 2,000 | 2,209 | 2,282 | 704 | 969 | 8,164 |
| 3AD | 416 | 581 | 315 | 90 | 123 | 1,525 |
| 4AD | 15 | 106 | 22 | 12 | 17 | 172 |
| 5AD | 0 | 21 | 4 | 1 | 2 | 28 |
| 6AD | 1 | 9 | 0 | 3 | 1 | 14 |
| 7AD | 0 | 1 | 0 | 0 | 0 | 1 |
| 9AD | 0 | 1 | 0 | 0 | 0 | 1 |

### `p`  (70,184 occurrences, 637 expansions)

**by province** (top 12)

| province | pedes | publi | publius | patriae | pia | total |
| --- | --- | --- | --- | --- | --- | --- |
| Roma | 3,950 | 2,504 | 2,553 | 242 | 18 | 9,267 |
| Venetia et Histria / Regio X | 2,437 | 724 | 398 | 51 | 1 | 3,611 |
| Latium et Campania / Regio I | 1,252 | 1,186 | 867 | 160 | 3 | 3,468 |
| Africa proconsularis | 7 | 238 | 519 | 482 | 824 | 2,070 |
| Germania superior | 7 | 78 | 50 | 70 | 1,438 | 1,643 |
| Numidia | 0 | 298 | 603 | 361 | 248 | 1,510 |
| Samnium / Regio IV | 322 | 423 | 254 | 25 | 1 | 1,025 |
| Etruria / Regio VII | 180 | 442 | 172 | 55 | 1 | 850 |
| Apulia et Calabria / Regio II | 321 | 244 | 193 | 88 | 0 | 846 |
| Gallia Narbonensis | 365 | 237 | 150 | 80 | 3 | 835 |
| Aemilia / Regio VIII | 448 | 203 | 99 | 24 | 2 | 776 |
| Provincia incerta | 11 | 212 | 26 | 291 | 124 | 664 |

**by century** (midpoint of the dating range)

| century | pedes | publi | publius | patriae | pia | total |
| --- | --- | --- | --- | --- | --- | --- |
| 3BC | 0 | 13 | 9 | 0 | 0 | 22 |
| 2BC | 7 | 59 | 63 | 0 | 0 | 129 |
| 1BC | 1,700 | 917 | 569 | 4 | 0 | 3,190 |
| 1AD | 3,185 | 1,998 | 1,479 | 466 | 56 | 7,184 |
| 2AD | 1,123 | 929 | 1,437 | 1,196 | 202 | 4,887 |
| 3AD | 150 | 195 | 403 | 1,328 | 145 | 2,221 |
| 4AD | 41 | 13 | 19 | 68 | 1 | 142 |
| 5AD | 50 | 0 | 2 | 0 | 0 | 52 |
| 6AD | 20 | 0 | 1 | 5 | 1 | 27 |

### `s`  (66,140 occurrences, 609 expansions)

**by province** (top 12)

| province | sacrum | situs | solvit | sita | sit | total |
| --- | --- | --- | --- | --- | --- | --- |
| Africa proconsularis | 7,980 | 4,154 | 517 | 2,670 | 207 | 15,528 |
| Numidia | 4,976 | 3,356 | 185 | 2,257 | 42 | 10,816 |
| Lusitania | 761 | 995 | 398 | 634 | 1,325 | 4,113 |
| Baetica | 758 | 804 | 56 | 633 | 1,506 | 3,757 |
| Hispania citerior | 440 | 659 | 451 | 288 | 553 | 2,391 |
| Mauretania Caesariensis | 1,290 | 351 | 73 | 167 | 134 | 2,015 |
| Roma | 918 | 138 | 93 | 68 | 76 | 1,293 |
| Pannonia superior | 75 | 293 | 573 | 69 | 11 | 1,021 |
| Dalmatia | 279 | 172 | 287 | 31 | 1 | 770 |
| Germania superior | 22 | 221 | 497 | 10 | 3 | 753 |
| Apulia et Calabria / Regio II | 140 | 329 | 23 | 224 | 2 | 718 |
| Latium et Campania / Regio I | 427 | 22 | 55 | 108 | 9 | 621 |

**by century** (midpoint of the dating range)

| century | sacrum | situs | solvit | sita | sit | total |
| --- | --- | --- | --- | --- | --- | --- |
| 2BC | 1 | 0 | 0 | 0 | 1 | 2 |
| 1BC | 6 | 51 | 32 | 99 | 5 | 193 |
| 1AD | 341 | 1,974 | 792 | 835 | 609 | 4,551 |
| 2AD | 3,617 | 1,812 | 2,420 | 1,201 | 951 | 10,001 |
| 3AD | 984 | 218 | 1,171 | 147 | 119 | 2,639 |
| 4AD | 120 | 9 | 12 | 3 | 4 | 148 |
| 5AD | 55 | 1 | 0 | 0 | 0 | 56 |
| 6AD | 19 | 0 | 1 | 0 | 0 | 20 |
| 7AD | 4 | 0 | 0 | 0 | 0 | 4 |

### `c`  (64,081 occurrences, 629 expansions)

**by province** (top 12)

| province | cai | caius | caio | curavit | clarissimo | total |
| --- | --- | --- | --- | --- | --- | --- |
| Roma | 6,132 | 5,180 | 1,577 | 204 | 425 | 13,518 |
| Latium et Campania / Regio I | 3,039 | 2,225 | 961 | 74 | 133 | 6,432 |
| Numidia | 580 | 2,017 | 218 | 16 | 68 | 2,899 |
| Venetia et Histria / Regio X | 1,542 | 825 | 419 | 18 | 19 | 2,823 |
| Africa proconsularis | 694 | 1,539 | 193 | 46 | 157 | 2,629 |
| Samnium / Regio IV | 887 | 606 | 300 | 28 | 16 | 1,837 |
| Etruria / Regio VII | 1,071 | 505 | 175 | 12 | 15 | 1,778 |
| Umbria / Regio VI | 810 | 473 | 220 | 16 | 12 | 1,531 |
| Gallia Narbonensis | 669 | 488 | 227 | 29 | 20 | 1,433 |
| Hispania citerior | 491 | 357 | 192 | 340 | 24 | 1,404 |
| Apulia et Calabria / Regio II | 497 | 427 | 238 | 32 | 32 | 1,226 |
| Lusitania | 191 | 235 | 61 | 647 | 4 | 1,138 |

**by century** (midpoint of the dating range)

| century | cai | caius | caio | curavit | clarissimo | total |
| --- | --- | --- | --- | --- | --- | --- |
| 3BC | 57 | 53 | 1 | 0 | 0 | 111 |
| 2BC | 191 | 147 | 9 | 1 | 0 | 348 |
| 1BC | 1,864 | 1,163 | 309 | 49 | 0 | 3,385 |
| 1AD | 4,928 | 3,932 | 1,817 | 514 | 2 | 11,193 |
| 2AD | 1,981 | 2,944 | 1,428 | 917 | 61 | 7,331 |
| 3AD | 345 | 937 | 699 | 270 | 174 | 2,425 |
| 4AD | 23 | 40 | 60 | 11 | 286 | 420 |
| 5AD | 3 | 1 | 3 | 0 | 198 | 205 |
| 6AD | 1 | 0 | 0 | 0 | 95 | 96 |

### `v`  (38,757 occurrences, 317 expansions)

**by province** (top 12)

| province | vixit | votum | vivus | vir | viro | total |
| --- | --- | --- | --- | --- | --- | --- |
| Numidia | 7,958 | 214 | 5 | 119 | 85 | 8,381 |
| Africa proconsularis | 3,495 | 513 | 7 | 118 | 220 | 4,353 |
| Roma | 2,938 | 111 | 243 | 497 | 461 | 4,250 |
| Venetia et Histria / Regio X | 33 | 516 | 540 | 44 | 22 | 1,155 |
| Mauretania Caesariensis | 734 | 79 | 3 | 29 | 22 | 867 |
| Latium et Campania / Regio I | 413 | 56 | 51 | 114 | 175 | 809 |
| Pannonia superior | 21 | 625 | 94 | 16 | 5 | 761 |
| Apulia et Calabria / Regio II | 610 | 25 | 14 | 27 | 45 | 721 |
| Gallia Narbonensis | 25 | 471 | 94 | 8 | 23 | 621 |
| Transpadana / Regio XI | 50 | 303 | 166 | 26 | 46 | 591 |
| Hispania citerior | 31 | 474 | 15 | 25 | 24 | 569 |
| Dalmatia | 96 | 308 | 102 | 24 | 27 | 557 |

**by century** (midpoint of the dating range)

| century | vixit | votum | vivus | vir | viro | total |
| --- | --- | --- | --- | --- | --- | --- |
| 2BC | 21 | 1 | 0 | 0 | 0 | 22 |
| 1BC | 216 | 37 | 110 | 0 | 0 | 363 |
| 1AD | 1,847 | 838 | 811 | 8 | 6 | 3,510 |
| 2AD | 2,988 | 2,580 | 382 | 37 | 72 | 6,059 |
| 3AD | 527 | 1,371 | 70 | 275 | 338 | 2,581 |
| 4AD | 265 | 17 | 6 | 469 | 368 | 1,125 |
| 5AD | 21 | 0 | 0 | 142 | 207 | 370 |
| 6AD | 7 | 1 | 0 | 70 | 103 | 181 |
| 7AD | 1 | 0 | 0 | 1 | 1 | 3 |
| 8AD | 0 | 0 | 0 | 1 | 0 | 1 |

### `a`  (30,461 occurrences, 419 expansions)

**by province** (top 12)

| province | annos | auli | aulus | animo | ante | total |
| --- | --- | --- | --- | --- | --- | --- |
| Numidia | 7,773 | 28 | 60 | 165 | 0 | 8,026 |
| Roma | 2,797 | 1,072 | 917 | 8 | 208 | 5,002 |
| Africa proconsularis | 3,114 | 29 | 63 | 427 | 2 | 3,635 |
| Latium et Campania / Regio I | 403 | 724 | 569 | 6 | 42 | 1,744 |
| Mauretania Caesariensis | 714 | 6 | 8 | 73 | 2 | 803 |
| Apulia et Calabria / Regio II | 602 | 86 | 64 | 2 | 1 | 755 |
| Etruria / Regio VII | 273 | 271 | 151 | 4 | 13 | 712 |
| Provincia incerta | 15 | 51 | 15 | 1 | 274 | 356 |
| Lusitania | 8 | 9 | 9 | 310 | 2 | 338 |
| Venetia et Histria / Regio X | 41 | 134 | 65 | 6 | 8 | 254 |
| Samnium / Regio IV | 115 | 56 | 51 | 2 | 3 | 227 |
| Gallia Narbonensis | 24 | 99 | 83 | 8 | 2 | 216 |

**by century** (midpoint of the dating range)

| century | annos | auli | aulus | animo | ante | total |
| --- | --- | --- | --- | --- | --- | --- |
| 3BC | 0 | 5 | 7 | 0 | 0 | 12 |
| 2BC | 25 | 30 | 25 | 0 | 23 | 103 |
| 1BC | 227 | 445 | 249 | 2 | 61 | 984 |
| 1AD | 1,897 | 590 | 524 | 64 | 70 | 3,145 |
| 2AD | 2,937 | 250 | 287 | 111 | 151 | 3,736 |
| 3AD | 395 | 20 | 41 | 77 | 56 | 589 |
| 4AD | 146 | 5 | 3 | 2 | 5 | 161 |
| 5AD | 15 | 0 | 1 | 0 | 0 | 16 |
| 6AD | 6 | 0 | 0 | 0 | 0 | 6 |
| 7AD | 1 | 0 | 0 | 0 | 1 | 2 |
| 17AD | 1 | 0 | 0 | 0 | 0 | 1 |

### `aug`  (28,162 occurrences, 58 expansions)

**by province** (top 12)

| province | augusti | augusto | augustae | augustus | augusta | total |
| --- | --- | --- | --- | --- | --- | --- |
| Roma | 3,699 | 578 | 455 | 132 | 61 | 4,925 |
| Africa proconsularis | 1,039 | 1,057 | 382 | 169 | 51 | 2,698 |
| Numidia | 569 | 808 | 699 | 109 | 46 | 2,231 |
| Latium et Campania / Regio I | 842 | 307 | 157 | 115 | 18 | 1,439 |
| Britannia | 142 | 99 | 87 | 45 | 1,041 | 1,414 |
| Provincia incerta | 322 | 53 | 33 | 316 | 93 | 817 |
| Hispania citerior | 224 | 297 | 55 | 130 | 57 | 763 |
| Germania superior | 73 | 109 | 175 | 23 | 229 | 609 |
| Mauretania Caesariensis | 163 | 198 | 53 | 85 | 23 | 522 |
| Gallia Narbonensis | 154 | 139 | 87 | 88 | 6 | 474 |
| Venetia et Histria / Regio X | 154 | 164 | 94 | 45 | 0 | 457 |
| Dacia | 217 | 80 | 57 | 50 | 35 | 439 |

**by century** (midpoint of the dating range)

| century | augusti | augusto | augustae | augustus | augusta | total |
| --- | --- | --- | --- | --- | --- | --- |
| 2BC | 1 | 0 | 0 | 0 | 0 | 1 |
| 1BC | 29 | 11 | 5 | 23 | 2 | 70 |
| 1AD | 1,706 | 447 | 318 | 333 | 53 | 2,857 |
| 2AD | 3,154 | 1,221 | 639 | 758 | 215 | 5,987 |
| 3AD | 1,553 | 1,804 | 568 | 852 | 113 | 4,890 |
| 4AD | 130 | 712 | 12 | 84 | 8 | 946 |
| 5AD | 19 | 66 | 0 | 2 | 0 | 87 |
| 6AD | 11 | 15 | 0 | 8 | 1 | 35 |
| 7AD | 0 | 4 | 0 | 2 | 0 | 6 |
| 20AD | 1 | 0 | 0 | 0 | 0 | 1 |

## 7. Metadata coverage (per extracted pair)

| metric | pairs | share of all pairs |
| --- | --- | --- |
| province present | 1,419,977 | 99.7% |
| date range present | 737,721 | 51.8% |
| both present | 736,196 | 51.7% |
| date resolves to a single century | 512,860 | 36.0% |

## 8. Parsing notes

- **`/` is a line break in EDCS, not a paired delimiter.** The brief asked to ignore text inside `/ /`; treating slashes as a pair would mask every other line of the corpus (200k+ single `/` against 55 `//` in a 60k sample). It is treated here as a hard token boundary instead, so no abbreviation is read across a line break. Square, angle and curly brackets ARE masked as spans.
- Brackets are unbalanced in roughly a fifth of texts (fragments). An unclosed `[` masks to the end of the string; a `]` with no opener masks from the start.
- Extraction is per whitespace token, not per `(...)` group, so `co(n)s(ul)` yields one pair (`cos` -> `consul`) rather than two fragments.
- `(?)` and `(!)` are editorial comments and are dropped; a trailing `?`/`!` *inside* an otherwise valid expansion (`dep(ositus?)`) is stripped and the pair kept.
- Frequency tables are case-folded; the TSV keeps the surface form.
- **Known artifact, not corrected here:** the doubled-letter plural. EDCS writes `DD(ominis)`, `CC(aiorum)`, `LL(uciorum)`, where the repeated letter marks a plural rather than starting the word. Concatenating gives `DDominis` instead of `dominis`. Roman numerals fused to a following abbreviation (`III(triere)`, `XX(vicesimae)`) fail the same way. Together these are ~3.8k pairs, 0.26% of the total; they are left in the TSV so they can be measured, and any real training set should filter or repair them.
- A few hundred pairs carry dates after 700 AD. Most are genuine early-medieval Christian inscriptions (`eps` -> episcopus, `scae` -> sanctae) that EDCS legitimately includes; a handful (e.g. 1998) are modern or mis-keyed. 446 pairs in total, 0.03%.

Pairs written to `data/derived/abbrev_pairs.tsv`.
