# The DD(ominis) artifact: diagnosis

Diagnosis only. `scripts/abbrev_probe.py` is imported, not modified, and nothing under `src/` or `data/` is touched.

## 0. Is this counting the same pairs the probe counts?

- records read: **588,509**
- pairs accepted by this walker: **1,424,314**
- pairs reported by the probe: **1,424,314**
- **match, so the numbers below describe the real dataset**

## 1. The three buckets

A token is a suspect when the letters before its first parenthesis are a repeated letter, a Roman numeral, or both. `mixed` exists because `DD`, `CC`, `LL`, `MM`, `XX` and `III` are simultaneously repeated letters and numeral characters -- there is no way to assign them to one bucket on shape alone.

Roman numerals are required to be upper case. EDCS carves numerals in capitals, and without that rule `vix(it)`, `mil(es)` and `civ(is)` -- whose letters are all numeral letters -- would be swept in as numbers.

| bucket | tokens | share of all pairs |
| --- | --- | --- |
| doubled | 30,498 | 2.14% |
| numeral | 586 | 0.04% |
| mixed | 3,409 | 0.24% |
| all suspects | 34,493 | 2.42% |

**These are suspects, not defects.** Section 2 shows why most of the `doubled` bucket is perfectly correct.

## 2. Thirty real examples per bucket

### bucket: `doubled`

| raw token | inscription id | current rule produces | inscription_text |
| --- | --- | --- | --- |
| `ann(is)` | EDCS-00000008-0 | `annis` | Dis Man(ibvs) sacr(vm) Postumia Cn(aei) f(ilia) Sperata sacerdos Cererum publica pia vixit ann(is) LXXI hic sita est |
| `cipp(us)` | EDCS-00000010-1 | `cippus` | R(ecto) r(igore) prox(imus) cipp(us) ped(es) XXXIII[?] |
| `ann(is)` | EDCS-00000033-0 | `annis` | Thallioni / delicato suo / qui vixit ann(is) / XV du[lc(issimo)] /puero / [ |
| `ann(os)` | EDCS-00000034-0 | `annos` | D(is) M(anibus) / Seppiae P(ubli) f(iliae) P(ubli) n(epoti) / P(ubli) pronepti Secun/dinae uxori ra/rissimae femi/nae innocen/tissimae / vixit ann(os) ... |
| `ann(is)` | EDCS-00000077-0 | `annis` | D(is) M(anibus) / Q(uinti) Quirini Ruf(i) / DEB (?) ROM (?) / AETER Imper(atoris) / M(arci) Aurel(i) / miles / ann(is) XXXXII / vix(it) / Ant(onius) h ... |
| `Succ(e)ssi` | EDCS-00000127-0 | `Successi` | Modestae / Succ(e)ssi f(iliae) |
| `ann(orum)` | EDCS-00000161-0 | `annorum` | D(is) M(anibus) / Cn(aeo) Seccio / Secundin(o) / def(uncto) ann(orum) XXIII / Cn(aeus) Seccius / Eutychys pa/trono et fra/tri b(ene) m(erenti) |
| `ann(os)` | EDCS-00000165-0 | `annos` | [D(is) M(anibus)] / Baleria Vi/ndemia Ata/nio Fyrmi/no coniugi / suo q(ui vixit) ann(os) XXXIII / m(enses) VI d(ies) VIII b(ene) m(erenti) / et sibi |
| `Terr(a)e` | EDCS-00000176-0 | `Terrae` | Terr(a)e M[a]/tri G(aius) Iul(ius) / Proc(u)lin[us] / v(otum) s(olvit) l(ibens) m(erito) |
| `ess(ent)` | EDCS-00000196-1 | `essent` | quor[um nomina subscripta sunt civitatem Romanam qui eorum non hab(erent) ded(it) et conubium cum] / ux(oribus) q(uas) t(unc) h[ab(uissent) cum est ci ... |
| `Syll(ectho)` | EDCS-00000200-0 | `Syllectho` | Mont[i] / a Syll(ectho) |
| `ann(os)` | EDCS-00000214-0 | `annos` | Antonius / Villaticus / fl(amen) p(er)p(etuus) vixit in / pace ann(os) L / dies XXVII hor(am) I |
| `nn(ostris)` | EDCS-00000215-0 | `nnostris` | [Impp(eratoribus) ? dd(ominis)] nn(ostris) / Flavio Va/lerio Con/stantio n/obilissim/o Caesari e[t] / Galerio Va/lerio Maxi/miano Iuni/ori nobilis/sim ... |
| `habuiss(ent)` | EDCS-00000245-2 | `habuissent` | Imp(erator) Caesar divi Hadriani f(ilius) divi Traiani / Parthic(i) nep(os) divi Nervae pronep(os) T(itus) Ael(ius) / Hadrianus Antoninus Aug(ustus) P ... |
| `duxiss(ent)` | EDCS-00000245-2 | `duxissent` | Imp(erator) Caesar divi Hadriani f(ilius) divi Traiani / Parthic(i) nep(os) divi Nervae pronep(os) T(itus) Ael(ius) / Hadrianus Antoninus Aug(ustus) P ... |
| `ann(orum)` | EDCS-00000327-0 | `annorum` | Cruaccus Civvo/nis fil(ius) ann(orum) LX / Clabruni Sammonis / filiae coniug(i) et parentib(us) |
| `ann(is)` | EDCS-00000493-0 | `annis` | D(is) M(anibus) / [3?] Trophimo / [v]ixit ann(is) XX / [I?]usta coniu[x] / [3]FE?[3] |
| `ann(os)` | EDCS-00000568-0 | `annos` | M(arcus) Camurius / M(arci) f(ilius) Pol(lia) Fortis / Regio mil(es) / coh(ortis) X urb(anae) / (centuria) Viri / militavit annos VIII / vix(it) ann(o ... |
| `dd(ominis)` | EDCS-00000572-0 | `ddominis` | Nymphis Aug(ustis) sac(rum) / salvis dd(ominis) nn(ostris) in/dulgentissimis div(is) / libenter pro salute / et adventu L(ucii) Eg/nati Victoris leg(a ... |
| `nn(ostris)` | EDCS-00000572-0 | `nnostris` | Nymphis Aug(ustis) sac(rum) / salvis dd(ominis) nn(ostris) in/dulgentissimis div(is) / libenter pro salute / et adventu L(ucii) Eg/nati Victoris leg(a ... |
| `Augg(ustorum)` | EDCS-00000572-0 | `Auggustorum` | Nymphis Aug(ustis) sac(rum) / salvis dd(ominis) nn(ostris) in/dulgentissimis div(is) / libenter pro salute / et adventu L(ucii) Eg/nati Victoris leg(a ... |
| `duxiss(ent)` | EDCS-00000581-1 | `duxissent` | [Imp(erator) Caes(ar) divi Hadriani f(ilius) divi Traian(i) Parthic(i) n(epos) divi Nervae pron(epos) T(itus) Aelius Hadrianus Antoninus Aug(ustus) Pi ... |
| `appell(antur)` | EDCS-00000619-0 | `appellantur` | Imp(erator) Caesar divi Traiani Parthici f(ilius) divi Ner/vae nepos Traianus Hadrianus Aug(ustus) / pontif(ex) max(imus) trib(unicia) potest(ate) XII ... |
| `miscell(anea)` | EDCS-00000619-0 | `miscellanea` | Imp(erator) Caesar divi Traiani Parthici f(ilius) divi Ner/vae nepos Traianus Hadrianus Aug(ustus) / pontif(ex) max(imus) trib(unicia) potest(ate) XII ... |
| `Pann(oniorum)` | EDCS-00000619-0 | `Pannoniorum` | Imp(erator) Caesar divi Traiani Parthici f(ilius) divi Ner/vae nepos Traianus Hadrianus Aug(ustus) / pontif(ex) max(imus) trib(unicia) potest(ate) XII ... |
| `Gall(orum)` | EDCS-00000619-0 | `Gallorum` | Imp(erator) Caesar divi Traiani Parthici f(ilius) divi Ner/vae nepos Traianus Hadrianus Aug(ustus) / pontif(ex) max(imus) trib(unicia) potest(ate) XII ... |
| `Pann(onica)` | EDCS-00000619-0 | `Pannonica` | Imp(erator) Caesar divi Traiani Parthici f(ilius) divi Ner/vae nepos Traianus Hadrianus Aug(ustus) / pontif(ex) max(imus) trib(unicia) potest(ate) XII ... |
| `dimiss(is)` | EDCS-00000619-0 | `dimissis` | Imp(erator) Caesar divi Traiani Parthici f(ilius) divi Ner/vae nepos Traianus Hadrianus Aug(ustus) / pontif(ex) max(imus) trib(unicia) potest(ate) XII ... |
| `habuiss(ent)` | EDCS-00000619-0 | `habuissent` | Imp(erator) Caesar divi Traiani Parthici f(ilius) divi Ner/vae nepos Traianus Hadrianus Aug(ustus) / pontif(ex) max(imus) trib(unicia) potest(ate) XII ... |
| `app(ellantur)` | EDCS-00000619-1 | `appellantur` | Imp(erator) Caes(ar) divi Trai(ani) Parth(ici) f(ilius) divi Nervae nepos / Trai(anus) Hadr(ianus) Aug(ustus) pontif(ex) max(imus) trib(unicia) pot(es ... |

### bucket: `numeral`

| raw token | inscription id | current rule produces | inscription_text |
| --- | --- | --- | --- |
| `CL(milibus)` | EDCS-01000017-0 | `CLmilibus` | [Haec] opera loc(ata) / [in refic(ienda) v]ia Caecilia de HS / [3 ad refic(iendum?) a]d mil(iarium) XXXV pontem in flu(v)io / [Farfaro pecuni]a a<t=D> ... |
| `XXI(locus)` | EDCS-01300026-0 | `XXIlocus` | [XXII(locus?)] / [3]a() / XXI(locus) P(ubli) Por(ci?) / C(ai) Val() |
| `XIX(locus)` | EDCS-01300026-3 | `XIXlocus` | XIX(locus) / T(iti) Va[3] / C(ai) Al[3] / Comini |
| `LVI(locus)` | EDCS-01300027-2 | `LVIlocus` | M(ani) Alleni / Capiton(is) / LVI(locus) |
| `VI(locus)` | EDCS-01600685-3 | `VIlocus` | VI(locus) / T(iti) Tit() / C(ai) Casn() |
| `VI(milibus)` | EDCS-02900188-0 | `VImilibus` | [1]aetiliae L(uci) f(iliae) / ex testamento / [3] Paroni L(uci) f(ilii) Fab(ia) / Cordi fili(i) / de HS VI(milibus) |
| `XL(quadragesimae)` | EDCS-03300644-0 | `XLquadragesimae` | XL(quadragesimae) Gall(iarum) st(atio) Mass(iliae) |
| `VI(vir)` | EDCS-03400130-0 | `VIvir` | D(is) M(anibus) s(acrum) / Baebiae Ursi/nae maritae / dulcissimae / et merentis/simae {H}I/lerdensi / Sul(picius) Primiti/v(u)s VI(vir) Tarra(conensis ... |
| `IV(milibus)` | EDCS-04001517-0 | `IVmilibus` | Mercurio / Aug(usto) sacr(um) / Q(uintus) Longeius Q(uinti) f(ilius) Pap(iria) / Faustinus aedil(is) praef(ectus) / i(ure) d(icundo) ob honorem aed(il ... |
| `LXXVI(milibus)` | EDCS-04201576-0 | `LXXVImilibus` | M(arcus) Doius M(arci) <f=P>(ilius) Ro[m(ilia)] / Clemens decur(io) adle[ctus] / quaest(or) II flamen / Augustalis ex HS LXXVI(milibus) / t(estamento) ... |
| `CL(milibus)` | EDCS-04700010-0 | `CLmilibus` | M(arcus) Quinc[tius?] / Fab(ia) Runco / praef(ectus) fabr(um) in[troit(um)] / et aedem Me[rcuri] / HS CL(milibus) res(tituenda) [cur(avit?)] / II(vir? ... |
| `LXVI(milia)` | EDCS-05100416-0 | `LXVImilia` | C(aius) Plinius L(uci) f(ilius) Ouf(entina) Caecilius [Secundus co(n)s(ul)] / augur legat(us) pro pr(aetore) provinciae Pon[ti et Bithyniae pro]/consu ... |
| `XL(quadragesimae)` | EDCS-05400458-0 | `XLquadragesimae` | ⟦I(ovi)] O(ptimo) M(aximo)⟧ / ⟦[T(itus) Fl]avius⟧ / ⟦[Aug(usti)] l(ibertus) Alypus⟧ / ⟦[3] XL(quadragesimae) Gal(l)i{c}(arum)⟧ / et / ⟦[Clau]dia Aug(u ... |
| `XL(quadragesimae)` | EDCS-05400463-0 | `XLquadragesimae` | ]us Au[g(usti) l(ibertus)] / [3 tab]ul(arius) XL(quadragesimae) / Gall(iarum) d(onum?) d(edit?) / [ |
| `XL(quadragesimae)` | EDCS-05401102-0 | `XLquadragesimae` | D(is) M(anibus) / Victori/naes(!) / Flaminalis / M(arci) Tarquini / Memoris \|(conductoris) XL(quadragesimae) / Gall(iarum) ser(vus) vilic(us) / statio ... |
| `XXC(milibus)` | EDCS-05502640-0 | `XXCmilibus` | P(ublius) Baebius Ve/nustus P(ubli) Bae/bi Veneti f(ilius) P(ubli) B/aebi Baesisce/ris nepos Or/etanus peten/te ordine et po/pulo in hon/orem domus /  ... |
| `VI(hexere)` | EDCS-05700231-0 | `VIhexere` | vix(it) annos] / XXXII mil(itavit) a[nnos 3] / h(eres) b(ene) m(erenti) f(ecit) / Antonius Lon/gus \|(centurio) VI(hexere) Ope |
| `VI(viri)` | EDCS-05700317-0 | `VIviri` | D(is) M(anibus) / L(uci) Carulli Fe/licissimi bis(elliarii) / VI(viri) Aug(ustalis) idem / q(uin)q(uennalis) L(aurentis) L(avinatis) q(uin)q(uennalis) ... |
| `VI(milia)` | EDCS-05700326-0 | `VImilia` | Nomina eorum qui pecuni[am 3] / et quibus dieb(us) natalis e[orum 3] / mens(is) Ian(uarius) / VIII Idus Ianuar(ias) / P(ubli) Cl(audi) Verati Abascant ... |
| `VI(milia)` | EDCS-05700326-0 | `VImilia` | Nomina eorum qui pecuni[am 3] / et quibus dieb(us) natalis e[orum 3] / mens(is) Ian(uarius) / VIII Idus Ianuar(ias) / P(ubli) Cl(audi) Verati Abascant ... |
| `VI(viri)` | EDCS-05700329-0 | `VIviri` | Di{i}s Manibus / Ti(beri) Claudi / Cumani l(iberti) Cerialis / VI(viri) Augustalis / Ostis / Claudia Primilla / opt<i=U>mo coniugi |
| `XV(milia)CC` | EDCS-05700375-0 | `XVmiliaCC` | P(ublio) Lucilio / P(ubli) f(ilio) P(ubli) n(epoti) P(ubli) pro/nep(oti) Gamalae / aed(ili) sacr(is) Vol<c=PK>(ani) (!) / [a]edili d(ecreto) d(ecurion ... |
| `VI(milibus)` | EDCS-05701321-0 | `VImilibus` | Ex testamento HS VI(milibus) arbitratu / T(iti) Manli T(iti) l(iberti) Niconis |
| `XV(milia)` | EDCS-05800076-0 | `XVmilia` | ] Dastidius C(ai) f(ilius) Celer pro honore ae[d(ilitatis)] / [C(aius)] Dastidius Sp(uri) f(ilius) Apollinaris pater pro honor[e] / flamoni(i) HS XV(m ... |
| `XL(milia)` | EDCS-05800999-0 | `XLmilia` | L(ucio) Urvineio L(uci) l(iberto) Philomuso / mag(istro) co<l=N>l(egii) libert(orum) / publice sepulturae et statuae in foro locus / datus est quod is ... |
| `IIID(milibus?)` | EDCS-06000122-0 | `IIIDmilibus` | ]LA[3] / [3 ob hon]orem augur[atus] / [3 summ]ae honoraria[e] / [3]t ex HS XX n(ummum) po/[suit 3]T Flavii Diodo/[3 adi]ectis HS IIID(milibus?) n(ummu ... |
| `VI(vir)` | EDCS-06100061-0 | `VIvir` | [A(ulus) Li]vius Anteros / [magiste]r quinquennal(is) colleg(ii) fabr(um) / [tignuari]orum Osti(en)s(ium) lustri XVII VI(vir) / [Augusta]lis corporatu ... |
| `XL(quadragesimae)` | EDCS-06100245-0 | `XLquadragesimae` | Invict[o Mithrae] / Bassus Ca[es(aris) ser(vus)] / circ(itor) XL(quadragesimae) G[all(iarum) Aug(ustae)] / Pra[et(oriae)] |
| `XI(I)` | EDCS-07400588-0 | `XII` | Imp(erator) Caes(ar) L(ucius) Sept(imius) / Severus Pius / Pertinax Aug(ustus) Arab(icus) / Adiabe(nicus) Parth(icus) max(imus) / pont(ifex) max(imus) ... |
| `XL(quadragennalibus)` | EDCS-07600459-2 | `XLquadragennalibus` | mul(tis) XL(quadragennalibus) |

### bucket: `mixed`

| raw token | inscription id | current rule produces | inscription_text |
| --- | --- | --- | --- |
| `DD(ominis)` | EDCS-00000894-0 | `DDominis` | DD(ominis) NN(ostris) Impp(eratoribus) Caes[s(aribus)] / Fl(avio) Constantino p(io) f(elici) / max(imo) victor<i=E> (!) ac trium/fatori semper Aug(ust ... |
| `DDD(ominis)` | EDCS-00000894-2 | `DDDominis` | DDD(ominis) NNN(ostris) / Gratiano / et Valenti/niano et{e} The/odosio se<m=N>per / Auggg(ustis) |
| `VII(I)` | EDCS-00001278-0 | `VIII` | Nymphis Iasis / Salutaribus / s[acrum] / Ianuari[us] / Augg(ustorum) / lib(ertus) ex / adiutor(e) tabul(ariorum) / prov(inciae) Pann(oniae) / Inf(erio ... |
| `DD(ominis)` | EDCS-00001486-0 | `DDominis` | DD(ominis) nn(ostris) Fl(avio) / Iul(io) Constan(tio) / et Fl(avio) Iul(io) Consta(nti) / Invictis Aug(ustis) / a Perge Me(tropoli) |
| `DD(ominorum)` | EDCS-00380180-0 | `DDominorum` | DD(ominorum) nn(ostrorum) Dioc/{c}letiani et Maxi/miani Augg(ustorum) et / Constanti et / Maximiani / nn(obilissimorum) Caess(arum) / m(ilia) p(assuum ... |
| `DD(ominis)` | EDCS-00381000-0 | `DDominis` | DD(ominis) nn(ostris) / Piissimis Felic[ibus] / perpetuis Imp[eratoribus] / Fl(avio) Val(erio) Consta[ntino] / Victori Maxim[o] / semp(er) Aug(usto) / ... |
| `XX(vicesimae)` | EDCS-00400035-0 | `XXvicesimae` | ]A[3] / [3 Ge]rm[ani]/[co Dacico et civitati] Ephes[i]/[orum A(ulus) Larcius(?) A(uli)] f(ilius) Palati/[na Crispinus pr]omag(ister) du/[u]m [p(ublico ... |
| `MM(arciorum)` | EDCS-00600281-0 | `MMarciorum` | M(arcus) Erucius M(arci) l(ibertus) Sabin[us] / M(arcus) Erucius M(arci) l(ibertus) Sinopa / M(arcus) Erucius M(arci) l(ibertus) Ru() / M(arcus) Eruci ... |
| `LL(uciorum)` | EDCS-00600690-0 | `LLuciorum` | Dis Manib(us) / Faenia L(uci) f(ilia) Syntyche [sibi et] / L(ucio) Faenio Carpo patri / Faeniae L(uci) lib(ertae) Helpidi mat[ri] / L(ucio) Faenio Ian ... |
| `CC(ai)` | EDCS-00600692-0 | `CCai` | [Dis] Manib(us) / [3]timiis Faenusae et / Eutychiae CC(ai) Iuli / Sextio et Receptus / fecerunt / sibi et suis libert(is) / libertabusque eius / poste ... |
| `LL(uciorum)` | EDCS-00600763-0 | `LLuciorum` | ]arri / [3] Faustus [3] / [3] Arriae L(uci) \|(mulieris) l(ibertae) [3] / [3] Arriae LL(uciorum) \|(mulieris) l(ibertae) Fa[3] / [3 A]rriae L(uci) \|(mul ... |
| `CCC(trecenario)` | EDCS-00900268-0 | `CCCtrecenario` | [Imp(eratori) Caesari] / [divi Antonini Pii] / [filio divi Ve]ri Parthic(i) / [maximi fr]atri divi / [Hadriani nep]ot(i) divi Traian(i) / [Parth(ici)  ... |
| `DDD(omini)` | EDCS-00900501-20 | `DDDomini` | DDD(omini) nnn(ostri) Fausta Constans Constantinus [3] |
| `DD(ominis)` | EDCS-00900549-0 | `DDominis` | DD(ominis) [nn(ostris) 3] / NI[3] / ⟦[6]⟧ / CE[ |
| `III(viro!)` | EDCS-01000208-0 | `IIIviro` | prov]incia[e 3] / [3 lega]to Achaiae leg(ato) Hisp(aniae) [ult(erioris) Baeticae] / [pr(aetori) tr(ibuno) pl(ebis) q(uaestori)] Galliae Narbonensis II ... |
| `II(locus)` | EDCS-01300025-1 | `IIlocus` | II(locus) / T(iti) Pum[3] / L(uci) Ves[3] |
| `XX(locus)` | EDCS-01300026-2 | `XXlocus` | XX(locus) / C(ai) Clu(vi?) / C(ai) Pomp() |
| `LL(uciorum)` | EDCS-01300289-0 | `LLuciorum` | ]DIO G[3] / Annio Prim[igenio?] / filio lib(erto) IIIIIIvir(o) / [A]nniae LL(uciorum) lib(ertae) PE[3] / Annio N[3] / Annio F[3] / L(ucio) Aterio N[3] ... |
| `XXX(milia)` | EDCS-01401130-0 | `XXXmilia` | L(ucius) Pellartius C(ai) (!) / Lem(onia) Celer Iulius Mon/tanus stipendior(um) XLIII / missus ex evocato et / armidoctor leg(ionis) XV Apol(linaris)  ... |
| `III(triere)` | EDCS-01401141-0 | `IIItriere` | Daza Pane/tis f(ilius) an(n)o(s) / vix(it) XXX mi/lit(avit) XVI III(triere) / C<ro=OR>codi/lo f(ecit) Plusia / lib(erta) patro(no) / suo et sibi |
| `DDD(ominis)` | EDCS-01401177-1 | `DDDominis` | DDD(ominis) nnn(ostris) / Valentiniano / Valenti et / Gratiano / perpetuis Piis / Felicibus sem/per Augustis |
| `CC(aesaribus)` | EDCS-01401179-0 | `CCaesaribus` | I[mp]p(eratoribus) CC(aesaribus) / dd(ominis) nn(ostris) / Valentiniano / et Valent<i=E> / se<m=N>/⟦3⟧per Augg(ustis) / insigne{m} / ortus felic<is=EM ... |
| `XX(vicesimae)` | EDCS-01600145-0 | `XXvicesimae` | Ti(berio) Claudio / Ti(beri) fil(io) Pal(atina) / Secundino / L(ucio) Statio Macedon[i] / p(rimo) p(ilo) leg(ionis) IIII F(laviae) F(elicis) trib(uno) ... |
| `XX(milia)` | EDCS-01600229-0 | `XXmilia` | M(arcus) Vocusius / M(arci) l(ibertus) Crescens / viv(us) fec(it) sib(i) et / Vocus(iae) Veneriae / coniug(i) optim(ae) / et Petronio / Vocusiano fil( ... |
| `III(triere)` | EDCS-01600237-0 | `IIItriere` | ] / signa [3] / guber[nator 3] / [3 de] III(triere) Corco[3] / [3]SIA[ |
| `XX(milia)` | EDCS-01600256-1 | `XXmilia` | si quis hanc arcam sive hoc / mon<u=I>ment(um) vendere aut emere / aut exacisclare volet tum / poenae nomine HS XX(milia) / r(ei) p(ublicae) Aquil(eie ... |
| `II(milia)` | EDCS-01600297-0 | `IImilia` | ] uxoris / [3 ma]rmoribus ex/[struxit cum sig]no aereo effi/[giei(?) 3 cum sig]nis marmoreis / [3]e verva aqua / [3 cas]tello(?) publico / [3]o et cet ... |
| `VII(locus)` | EDCS-01600685-2 | `VIIlocus` | VII(locus) / M(arci) Tet() / L(uci) Luc() |
| `DD(ominis)` | EDCS-01800005-0 | `DDominis` | DD(ominis) nn(ostris) / Fl(avio) Valentiniano / et Fl(avio) Valenti / Victorr(iosissimis) / Augg(ustis) |
| `VIII(milia)` | EDCS-02500004-0 | `VIIImilia` | [DDD(omini) nnn(ostri)] Auggg(usti) Valentinian[us Valens] Grati[a]nus ha<v=B>(e) Eutropi car(issime) nobis / [quod ex red]itibus fundorum iuris re[i  ... |

## 3. Which suspects are actually broken

For each suspect, two candidate readings are checked against the control lexicon (expansions produced by non-suspect tokens only, so the test cannot feed on itself):

- **naive** -- what the current rule produces (`ann`+`os` = `annos`, `dd`+`ominis` = `ddominis`)
- **collapsed** -- the doubled run reduced to one letter (`d`+`ominis` = `dominis`)
- **bare** -- for numerals, the parenthesis content alone (`vicesimae`)

| bucket | verdict | tokens |
| --- | --- | --- |
| doubled | correct_as_is | 21,442 |
| doubled | geminatio | 8,034 |
| doubled | unresolved | 1,022 |
| numeral | correct_as_is | 10 |
| numeral | numeral_word | 539 |
| numeral | unresolved | 37 |
| mixed | correct_as_is | 44 |
| mixed | geminatio | 1,107 |
| mixed | numeral_word | 1,995 |
| mixed | unresolved | 263 |

| verdict | tokens | share of suspects |
| --- | --- | --- |
| correct_as_is | 21,496 | 62.32% |
| geminatio | 9,141 | 26.50% |
| numeral_word | 2,534 | 7.35% |
| unresolved | 1,322 | 3.83% |

### How firm is each call?

A verdict is only as good as the gap between the winning reading and the runner-up. Decisions where the winner has at least three times the support of the next candidate are counted as decisive; the rest are close calls and should be treated as provisional.

| confidence | tokens | share of decided |
| --- | --- | --- |
| decisive (winner >= 3x runner-up) | 32,889 | 99.15% |
| thin (winner < 3x runner-up) | 282 | 0.85% |

Close calls, most frequent first:

| token | verdict | occurrences |
| --- | --- | --- |
| `nn(os)` | geminatio | 31 |
| `Poll(ae)` | geminatio | 23 |
| `II(virum)` | correct_as_is | 19 |
| `II(virorum)` | correct_as_is | 18 |
| `Ann(io)` | correct_as_is | 13 |
| `Avill(i)` | geminatio | 12 |
| `Att(ius)` | correct_as_is | 10 |
| `XX(librae)` | geminatio | 10 |
| `XXX(librae)` | geminatio | 8 |
| `MM(arcis)` | geminatio | 7 |
| `II(viro)` | numeral_word | 5 |
| `Murr(ius)` | correct_as_is | 5 |

## 4. Testing the geminatio hypothesis

### 4a. The plural-ending heuristic (weak evidence)

| group | tokens | plural-looking | share | of which on an ambiguous ending |
| --- | --- | --- | --- | --- |
| geminatio (corrected reading) | 9,141 | 8,793 | 96.19% | 41.46% |
| correct-as-is control group | 21,496 | 7,629 | 35.49% | 56.06% |

**This heuristic is not trustworthy on its own and the numbers above should not be quoted as a plural rate.** `-is` is the plural dative/ablative in *dominis* but the genitive singular in *civitatis*; `-i`, `-a` and `-um` are each singular at least as often as plural. The column showing how much of the signal rests on those ambiguous endings is there to make the weakness visible. The contrast in 4b is the evidence worth believing.

### 4b. Doubled form vs single form, side by side (strong evidence)

If doubling marks a plural, then for the same stem the doubled abbreviation should carry plural expansions and the single one singular expansions. This compares them directly and needs no ending list.

| doubled abbrev | its corrected expansions | single abbrev | its expansions |
| --- | --- | --- | --- |
| augg (n=1,793) | augustorum (1,329), augustis (396), augusti (59) | aug | augusti (10,509), augusto (6,269), augustae (3,077) |
| nn (n=1,702) | nostrorum (879), nostris (699), nostri (61) | n | nostri (3,471), nostro (1,644), numero (1,450) |
| dd (n=1,054) | dominis (675), dominorum (290), domini (51) | d | dis (54,049), dies (5,592), decreto (2,833) |
| cc (n=447) | clarissimis (138), caiorum (110), clarissimorum (99) | c | cai (21,717), caius (19,498), caio (6,665) |
| caess (n=400) | caesaribus (235), caesarum (118), caesares (45) | caes | caesari (2,299), caesaris (1,952), caesar (1,491) |
| conss (n=399) | consulibus (356), consulatu (22), consulatum (13) | cons | consulibus (231), consule (215), consulatu (170) |
| impp (n=387) | imperatoribus (194), imperatorum (160), imperatores (32) | imp | imperator (4,372), imperatori (4,034), imperatoris (2,830) |
| vv (n=299) | viris (149), virorum (115), viri (20) | v | vixit (17,469), votum (7,925), vivus (1,997) |
| nnn (n=297) | nostris (140), nostrorum (120), nostri (33) | n | nostri (3,471), nostro (1,644), numero (1,450) |
| ddd (n=253) | dominis (133), dominorum (90), domini (28) | d | dis (54,049), dies (5,592), decreto (2,833) |
| pp (n=204) | publiorum (59), piis (57), publi (13) | p | pedes (10,705), publi (8,813), publius (7,773) |
| auggg (n=187) | augustorum (116), augustis (59), augusti (12) | aug | augusti (10,509), augusto (6,269), augustae (3,077) |
| nobb (n=156) | nobilissimis (108), nobilissimi (29), nobilissimorum (18) | nob | nobilissimo (186), nobilissimus (27), nobilissimis (25) |
| mm (n=140) | marcorum (62), marci (34), manibus (23) | m | manibus (55,028), marcus (15,928), marci (15,502) |
| ll (n=137) | luciorum (90), luci (25), libentes (4) | l | luci (21,414), lucius (18,241), libertus (10,799) |

## 5. The reverse case: doubled letters that are just spelling

These tokens end in a doubled letter and the current rule handles them **correctly**. They are the reason a pattern match on doubled letters is not a bug detector: the double n in *annos* and the double s in *dulcissimae* belong to the word.

| token | occurrences |
| --- | --- |
| `ann(os)` | 10,498 |
| `ann(orum)` | 2,879 |
| `ann(is)` | 1,393 |
| `Off(icina)` | 447 |
| `Lucill(ae)` | 186 |
| `ann(o)` | 169 |
| `off(icina)` | 168 |
| `ann(um)` | 161 |
| `coll(egii)` | 138 |
| `Gall(orum)` | 136 |
| `miss(ione)` | 92 |
| `Ann(i)` | 84 |
| `ann(o)s` | 80 |
| `dulciss(imae)` | 70 |
| `dulciss(imo)` | 69 |
| `Pann(oniorum)` | 64 |
| `vacc(am)` | 62 |
| `duxiss(ent)` | 59 |
| `dimiss(is)` | 57 |
| `pientiss(imo)` | 56 |
| `coll(egium)` | 56 |
| `class(is)` | 55 |
| `Comm(odi)` | 55 |
| `appell(antur)` | 53 |
| `kariss(imae)` | 53 |

A second trap sits in multi-parenthesis tokens: `d(e)d(icavit)` produces the abbreviation `dd`, which looks like geminatio but expands correctly to *dedicavit*. This is why suspects are detected on the letters before the **first** parenthesis rather than on the assembled abbreviation.

## 6. Exact recount of affected pairs

| measure | pairs | share of all 1,424,314 pairs |
| --- | --- | --- |
| confirmed broken (geminatio + numeral) | 11,675 | 0.82% |
| unresolved, cannot be decided from the corpus | 1,322 | 0.09% |
| worst case, if every unresolved case is broken | 12,997 | 0.91% |
| earlier estimate in the probe report | 3,753 | 0.26% |

### by bucket

| bucket | affected pairs | share of affected |
| --- | --- | --- |
| doubled | 8,034 | 68.81% |
| numeral | 539 | 4.62% |
| mixed | 3,102 | 26.57% |

### by distinct abbreviation form

| abbrev | affected pairs | current (broken) expansion | corrected reading |
| --- | --- | --- | --- |
| Augg | 1,793 | Auggustorum, Auggustis | augustorum, augustis |
| nn | 1,694 | nnostrorum, nnostris | nostrorum, nostris |
| III | 637 | IIItriere, IIImilia | itriere, imilia |
| dd | 622 | ddominis, ddominorum | dominis, dominorum |
| DD | 421 | DDominis, DDominorum | dominis, dominorum |
| Caess | 398 | Caessaribus, Caessarum | caesaribus, caesarum |
| conss | 397 | conssulibus, conssulatu | consulibus, consulatu |
| Impp | 386 | Impperatoribus, Impperatorum | imperatoribus, imperatorum |
| II | 365 | IImilia, IIlibrae | imilia, ilibrae |
| vv | 296 | vviris, vvirorum | viris, virorum |
| nnn | 288 | nnnostris, nnnostrorum | nostris, nostrorum |
| cc | 274 | cclarissimis, cclarissimorum | clarissimis, clarissimorum |
| XX | 251 | XXvicesimae, XXmilia | xvicesimae, xmilia |
| CC | 206 | CCaiorum, CCaesaribus | caiorum, caesaribus |
| Auggg | 187 | Augggustorum, Augggustis | augustorum, augustis |
| nobb | 156 | nobbilissimis, nobbilissimi | nobilissimis, nobilissimi |
| IIII | 151 | IIIImilia, IIIImilibus | imilia, ilibrae |
| DDD | 139 | DDDominis, DDDominorum | dominis, dominorum |
| PP | 135 | PPubliorum, PPiis | publiorum, piis |
| MM | 130 | MMarcorum, MMarci | marcorum, marci |
| VI | 125 | VImilia, VImilibus | vimilia, vimilibus |
| LL | 121 | LLuciorum, LLuci | luciorum, luci |
| ddd | 109 | dddominorum, dddominis | dominorum, dominis |
| QQ | 74 | QQuintorum, QQuinti | quintorum, quinti |
| pp | 67 | pposuerunt, ppedes | posuerunt, pedes |
| VIII | 67 | VIIImilia, VIIImilibus | vimilia, vimilibus |
| XXX | 65 | XXXmilia, XXXmilibus | xmilia, xmilibus |
| FF | 61 | FFelicibus, FFelices | felicibus, felices |
| IV | 53 | IVmilia, IVscripula | ivmilia, ivscripula |
| VII | 51 | VIImilia, VIIlibrae | vimilia, vilibrae |
| XL | 46 | XLquadragesimae, XLmilia | xlquadragesimae, xlmilia |
| magg | 43 | maggistri, maggistratibus | magistri, magistratibus |
| XII | 43 | XIImilia, XIImilibus | ximilia, ximilibus |
| ff | 42 | ffecerunt, ffilii | fecerunt, filii |
| nnnn | 40 | nnnnostris, nnnnostrorum | nostris, nostrorum |
| eqq | 35 | eqquitum, eqquites | equitum, equites |
| TT | 35 | TTitorum, TTiti | titorum, titi |
| ss | 33 | ssolverunt, ssaribus | solverunt, saribus |
| XV | 30 | XVmilia, XVmilibus | xvmilia, xvmilibus |
| praeff | 27 | praeffectorum, praeffecti | praefectorum, praefecti |

_527 distinct abbreviation forms are affected in total._

### by province, against the corpus baseline

`lift` is the province's share of affected pairs divided by its share of all pairs. 1.0 means the artifact is spread exactly like the corpus; above ~2 means it concentrates there.

| province | affected | share of affected | share of corpus | lift |
| --- | --- | --- | --- | --- |
| Roma | 3,045 | 26.08% | 22.62% | 1.15 |
| Latium et Campania / Regio I | 927 | 7.94% | 8.79% | 0.90 |
| Africa proconsularis | 812 | 6.96% | 8.57% | 0.81 |
| Numidia | 674 | 5.77% | 7.72% | 0.75 |
| Aemilia / Regio VIII | 597 | 5.11% | 1.34% | 3.82 |
| Asia | 435 | 3.73% | 0.52% | 7.11 |
| Mauretania Caesariensis | 281 | 2.41% | 1.79% | 1.34 |
| Venetia et Histria / Regio X | 275 | 2.36% | 3.42% | 0.69 |
| Apulia et Calabria / Regio II | 249 | 2.13% | 1.66% | 1.29 |
| Moesia inferior | 236 | 2.02% | 1.03% | 1.97 |
| Britannia | 227 | 1.94% | 1.90% | 1.03 |
| Dalmatia | 184 | 1.58% | 2.02% | 0.78 |
| Hispania citerior | 182 | 1.56% | 3.18% | 0.49 |
| Etruria / Regio VII | 181 | 1.55% | 1.80% | 0.86 |
| Pontus et Bithynia | 170 | 1.46% | 0.22% | 6.70 |
| Pannonia inferior | 170 | 1.46% | 1.43% | 1.02 |
| Galatia | 170 | 1.46% | 0.39% | 3.78 |
| Gallia Narbonensis | 169 | 1.45% | 2.49% | 0.58 |
| Pannonia superior | 165 | 1.41% | 2.00% | 0.71 |
| Provincia incerta | 156 | 1.34% | 1.85% | 0.72 |

### by century, against the corpus baseline

| century | affected | share of affected | share of corpus | lift |
| --- | --- | --- | --- | --- |
| 2BC | 2 | 0.02% | 0.26% | 0.07 |
| 1BC | 121 | 1.04% | 3.00% | 0.35 |
| 1AD | 442 | 3.79% | 13.33% | 0.28 |
| 2AD | 1,040 | 8.91% | 19.84% | 0.45 |
| 3AD | 2,059 | 17.64% | 9.96% | 1.77 |
| 4AD | 2,232 | 19.12% | 3.80% | 5.03 |
| 5AD | 520 | 4.45% | 0.87% | 5.14 |
| 6AD | 69 | 0.59% | 0.51% | 1.15 |
| 7AD | 6 | 0.05% | 0.09% | 0.60 |
| (undated) | 5,184 | 44.40% | 48.21% |  |

### by inscription category

This is the bias question that matters: if the artifact sits inside imperial and military texts, discarding it quietly removes that stratum.

| category | affected | share of affected | share of corpus | lift |
| --- | --- | --- | --- | --- |
| men | 7,973 | 17.36% | 17.83% | 0.97 |
| emperor/emperess | 5,220 | 11.36% | 3.61% | 3.15 |
| tria nomina | 4,275 | 9.31% | 13.78% | 0.68 |
| tomb inscriptions | 2,633 | 5.73% | 14.16% | 0.40 |
| women | 2,278 | 4.96% | 8.24% | 0.60 |
| milestones | 2,210 | 4.81% | 1.00% | 4.80 |
| dedicatory inscriptions | 2,053 | 4.47% | 4.48% | 1.00 |
| building inscriptions | 2,018 | 4.39% | 2.83% | 1.55 |
| single name | 1,897 | 4.13% | 4.47% | 0.92 |
| freed men and women | 1,415 | 3.08% | 3.35% | 0.92 |
| honorary inscriptions | 1,346 | 2.93% | 1.64% | 1.78 |
| manufacturer's inscriptions | 1,324 | 2.88% | 3.55% | 0.81 |
| christian inscriptions | 1,255 | 2.73% | 1.46% | 1.87 |
| erased inscriptions | 1,245 | 2.71% | 0.91% | 2.98 |
| soldiers | 1,234 | 2.69% | 4.06% | 0.66 |

### 6b. A blind spot one character below the threshold

The three buckets require at least two letters before the parenthesis. That threshold exists for a good reason -- `D(is)`, `M(anibus)`, `C(aius)` and `L(ucius)` are ordinary abbreviations that happen to be numeral letters, and there are 271,012 such tokens. Bucketing them would be a catastrophe.

But the same artifact does occur there: `X(milia)` produces `Xmilia`. These cannot be found by the arbitration above, because with nothing to collapse their broken reading is the only reading in the control lexicon -- indeed `X(milia)` is *why* `xmilia` had 108 attestations, which is what made the first version of this diagnostic mis-rule `XX(milia)` as geminatio.

The test used here is independent support: flag a single-numeral token when its naive reading is attested **only** by tokens of the same shape, while the parenthesis content on its own is attested elsewhere.

- single-numeral-prefix tokens scanned: **271,012**
- flagged by this test: **753** (0.05% of all pairs)

| token | current output | its support (all self) | bare reading | its independent support | occurrences |
| --- | --- | --- | --- | --- | --- |
| `D(quingenaria)` | `dquingenaria` | 89 | `quingenaria` | 1 | 89 |
| `L(milia)` | `lmilia` | 84 | `milia` | 1,725 | 83 |
| `V(milia)` | `vmilia` | 66 | `milia` | 1,725 | 62 |
| `I(libra)` | `ilibra` | 66 | `libra` | 14 | 53 |
| `C(milia)` | `cmilia` | 53 | `milia` | 1,725 | 52 |
| `V(librae)` | `vlibrae` | 42 | `librae` | 11 | 40 |
| `X(librae)` | `xlibrae` | 41 | `librae` | 11 | 40 |
| `X(milibus)` | `xmilibus` | 27 | `milibus` | 160 | 26 |
| `V(milibus)` | `vmilibus` | 20 | `milibus` | 160 | 20 |
| `V(atiam)` | `vatiam` | 17 | `atiam` | 1 | 17 |
| `C(aiae)` | `caiae` | 17 | `aiae` | 1 | 14 |
| `I(Libra)` | `ilibra` | 66 | `libra` | 14 | 13 |
| `I(mille)` | `imille` | 12 | `mille` | 70 | 12 |
| `I(uncia)` | `iuncia` | 15 | `uncia` | 5 | 11 |
| `C(milibus)` | `cmilibus` | 11 | `milibus` | 160 | 10 |
| `L(librae)` | `llibrae` | 10 | `librae` | 11 | 10 |
| `I(libram)` | `ilibram` | 10 | `libram` | 11 | 10 |
| `I(assem)` | `iassem` | 9 | `assem` | 17 | 9 |
| `L(milibus)` | `lmilibus` | 9 | `milibus` | 160 | 8 |
| `V(unciae)` | `vunciae` | 9 | `unciae` | 7 | 8 |

**Read this number with more caution than the others.** The genuine cases are the numeral-plus-measure ones -- *milia*, *librae*, *milibus*, *uncia*, *assem*, *mille* -- where a Roman numeral is followed by the unit it counts. Mixed in are false positives on names, where the naive reading is correct and merely rare: `C(aiae)` is *Caiae*, `V(atiam)` is *Vatiam*, `L(a)elia` is *Laelia*. Eyeballing the list puts the genuine share somewhere around four-fifths, but that is an impression, not a measurement, so this figure is reported beside the headline count rather than added into it.

## 7. Do these forms contaminate the probe's tables?

- affected abbreviation forms appearing in the **top-50 frequency table**: **2** (`ann`, `cl`)
- affected forms that currently register as **ambiguous** and whose expansion count would change once corrected: **10**

| abbrev | freq | distinct expansions now | after correction | broken expansions |
| --- | --- | --- | --- | --- |
| ann | 15,379 | 26 | 24 | annae, annnorum, annnos |
| pp | 840 | 71 | 68 | pparentes, pparia, pparibus |
| qq | 836 | 36 | 34 | qquaestionarii, qquaestores, qquaestoribus |
| off | 677 | 15 | 14 | offficina |
| conss | 419 | 9 | 8 | conssulatu, conssulatum, conssule |
| vv | 332 | 24 | 23 | vviatores, vvibiorum, vvicit |
| pann | 151 | 11 | 10 | pannam, pannnoniarum |
| ss | 116 | 43 | 41 | ssacris, ssacrum, ssalvis |
| tt | 55 | 9 | 8 | ttiti, ttitis, ttitorum |
| viivir | 39 | 4 | 3 | viiiviro |

There is a second, subtler effect. Correcting by collapsing the repeat moves these pairs onto a **different abbreviation key**: every `dd` pair becomes a `d` pair. That does not just clean up `dd`, it enlarges the expansion set of `d`, `c`, `l`, `m`, `n` and `aug` -- the highest-frequency entries in the ambiguity table. The correction therefore changes the headline ambiguity numbers in both directions, and any fix should be followed by a re-run rather than an adjustment of the existing figures.

| affected form | pairs | would merge into | that form's current freq | its current distinct expansions |
| --- | --- | --- | --- | --- |
| augg | 1,793 | aug | 28,162 | 58 |
| nn | 1,702 | n | 14,299 | 295 |
| dd | 1,054 | d | 87,666 | 399 |
| iii | 637 | i | 7,206 | 225 |
| cc | 481 | c | 64,081 | 629 |
| caess | 400 | caes | 6,339 | 32 |
| conss | 399 | cons | 1,117 | 51 |
| impp | 387 | imp | 12,641 | 27 |
| ii | 366 | i | 7,206 | 225 |
| vv | 300 | v | 38,757 | 317 |
| nnn | 297 | n | 14,299 | 295 |
| ddd | 253 | d | 87,666 | 399 |

## 8. Three handling strategies (proposed, not applied)

### A. Drop the affected pairs

**For:** one filter, no linguistic judgement, no risk of inventing a reading the editor did not intend. The numeral cases in particular have no single correct concatenation, so dropping them is honest.

**Against:** it deletes a stratum rather than a random sample. The imperial titulature that carries geminatio -- *dominis nostris*, *Augustorum*, *Impp(eratoribus)* -- is exactly the material a model would need to learn imperial formulae, and section 6 shows where it concentrates. It also throws away the plural information the doubling encodes.

### B. Collapse the repeat and keep the singular stem

**For:** produces the philologically correct string (*dominis*, not *DDominis*), and the attestation test in section 3 confirms the collapsed reading against expansions the corpus already contains, so it is checkable rather than assumed.

**Against:** it silently discards the plural marking, which is real information -- `dd nn` means *two* emperors, and after collapsing, `dd` and `d` become indistinguishable. It also rewrites the abbreviation key and so reshuffles the ambiguity tables (section 7). It does nothing for the numeral bucket, where there is no repeat to collapse.

### C. Keep them, corrected, with a plurality flag

**For:** keeps every pair, records the correct expansion, and preserves the geminatio as an explicit feature (`plural=True`, `marker=dd`) instead of destroying or ignoring it. Downstream work can filter on the flag, so this strategy contains both of the others -- A and B remain available as filters over a flagged dataset, while the reverse is not true.

**Against:** it costs two new columns in the TSV and a documented convention, and the numeral bucket still needs its own rule (the parenthesis content replaces the numeral rather than continuing it). The unresolved residue in section 3 has to be labelled as unresolved rather than guessed.

## 9. Verdict

**Bug or convention?** Both, and the split is clean. 21,496 of the 34,493 suspect tokens (62.32%) are handled correctly right now -- the doubled letter is ordinary spelling, as in *annos* and *dulcissimae*. The remainder are two distinct EDCS conventions the rule does not know about: geminatio marking a plural (9,141 pairs) and a Roman numeral standing for a word (2,534 pairs). The concatenation rule is not wrong in general; it is wrong for these two conventions.

**True affected count:** **11,675 pairs (0.82% of the corpus)**, plus 1,322 unresolved, giving a worst case of 12,997. The earlier 3,753 / 0.26% estimate was low, because its pattern required upper-case numeral letters and so missed every lower-case geminatio -- `dd(ominis)`, `nn(ostris)`, `augg(ustorum)`, `impp(eratoribus)`, `conss(ulibus)` -- which are the bulk of the phenomenon.

**Bias: yes, strongly, and in a way that matters.** This is not spread evenly across the corpus.

- By period: 5AD lift 5.1x; 4AD lift 5.0x; 3AD lift 1.8x -- against 2BC 0.07x; 1AD 0.28x; 1BC 0.35x. Geminatio marks *co-rule*, so it tracks the periods with more than one emperor -- the tetrarchy and after. The artifact is effectively a late-antique stratum.
- By category: milestones lift 4.8x; emperor/emperess lift 3.2x; erased inscriptions lift 3.0x; legal incriptions lift 2.8x. Tomb inscriptions, the corpus's largest genre, are the reverse at 0.40x.

Dropping these pairs would therefore not remove a random 0.8%. It would preferentially delete imperial titulature, milestones and 4th-5th century material while leaving the funerary bulk untouched -- a systematic thinning of exactly the formulaic, well-dated material that a disambiguation experiment would most want to condition on.

**Recommendation: C, keep them with a corrected expansion and a plurality flag.** The affected share is small enough that neither dropping nor collapsing would move headline accuracy much, which is precisely why the decision should be made on information rather than convenience. Geminatio is not noise -- it is the corpus telling you how many emperors were reigning, and it is concentrated in the imperial and military formulae that a disambiguation model most needs to see. Strategy C is also the only one that is reversible: a flagged dataset can be filtered down to A or B later, whereas dropped or collapsed pairs cannot be recovered without another full re-extraction.

