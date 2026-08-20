#!/usr/bin/env python3
"""Phase 3b: the explicit numeral word lists, printed and versioned.

The brief requires these to be EXPLICIT lists with reported coverage, not a
heuristic. They are built from the complete evidence base in
scripts/phase3_numeral_evidence.py -- every parenthesis content that follows
an all-Roman-numeral prefix anywhere in the kept 1,424,314 pairs -- and every
entry is a decision recorded here rather than a threshold applied silently.

Assigning a Latin word to a semantic class is a philological judgement.
[VERIFY -- LATINIST] applies to every list below. What does NOT need a
Latinist is the coverage report: how many pairs the lists reach and how many
are left UNRESOLVED. Anything the lists miss stays UNRESOLVED. Nothing is
guessed.
"""

# TYPE 1 -- the numeral STANDS FOR the word. Expansion = bracket content alone.
#   XL(quadragesimae) -> quadragesimae, not "XLquadragesimae".
# Ordinals and number-derived substantives: tax names (vicesima = the 5% tax),
# imperial anniversaries (vicennalia = 20th year), procuratorial pay grades
# named for their salary in thousands of sesterces (ducenarius = 200,000).
TYPE1_NUMERAL_WORD = {
    # ordinals and cardinals used substantively
    "secundum", "secundus", "tertia", "tertium", "tertius", "quarta", "quartum",
    "sexta", "octava", "novem", "duo", "duorum", "duobus", "trium", "bis",
    "sexaginta", "centena", "ternis", "decima", "decimam", "quinta",
    # fiscal ordinals (the vicesima hereditatium, the quadragesima portoria)
    "vicesima", "vicesimae", "vicesimam", "vicesiame",
    "quadragesimae", "quadragesima", "quinquagesimae", "centesima", "centesimis",
    "millesima", "millesimam",
    # imperial anniversary formulae
    "vicennalibus", "vicennalia", "vicennalium",
    "tricennalibus", "trecennalibus", "quadragennalibus", "decennalibus",
    # pay-grade titles named for a salary in thousands of sesterces
    "ducenario", "ducenarius", "ducenarium", "ducenarii",
    "trecenario", "trecenarius", "trecenarii", "quadringenarii",
    "centenarius", "centenaria", "sexagenarius",
}

# TYPE 2 -- numeral + SUPPLIED UNIT. The word is nowhere on the stone; on the
#   stone it was an overline or a sign (Phase 2, D-0017). Store the numeral and
#   the supplied word in separate fields. Class = numeral_ellipsis. Kept in the
#   release, EXCLUDED from the abbreviation task.
TYPE2_SUPPLIED_UNIT = {
    # the thousands device (D-0017)
    "milia", "milibus", "milium", "mille", "milibusm",
    # weight: the libra and its parts
    "libra", "librae", "libram", "libras", "librarum", "libris",
    "uncia", "unciae", "uncias", "unciis", "unciarum",
    "semuncia", "semunciae", "semuncias", "sicilicus", "sicilici",
    "scripulum", "scripula", "scripuli", "scripulae", "scripulis", "scriptulis",
    "sextula", "sextulae", "duella", "deunx", "dodrans", "dodrantem",
    "dextans", "dextantes", "bes", "bessem", "septunx", "quincunx",
    "triens", "trientem", "quadrans", "quadrantem", "quadrantes",
    "sextans", "sextantem", "semis", "sescuncia",
    # coin and money of account
    "assem", "asses", "assibus", "assium", "as",
    "denarii", "denarius", "denariis", "denarios", "denariorum", "denarium",
    "sestertium", "sestertius", "sestertia", "sestertiis",
    "solidi", "solidum", "siliquae", "didrachma", "libellas", "libellae",
    "dupondius", "nummum",
    # capacity, area, length
    "sextarii", "sextarius", "sextarium", "sextariorum", "sestarios",
    "modii", "modiorum", "congios", "hemina", "medimna",
    "iugera", "pedes", "passus", "pondera", "pondus",
    # counted objects and events (the noun is supplied, the numeral counts it)
    "vasa", "ollae", "dolium", "locus", "loca",
    "coronarum", "corona", "coronae", "pugnarum", "pugna", "pugnae",
    "victoriarum", "capitibus", "stipendiorum",
}

# TYPE 3 -- numeral as WORD-PREFIX. The numeral is the first element of a
#   compound word. Gold label = the EDCS SURFACE FORM (VIvir), because that is
#   what is verifiable against the source. The Latin reading (sevir) goes in
#   normalized_form ONLY. A gold label must never depend on a contested reading.
TYPE3_NUMERAL_PREFIX = {
    # the -vir offices: duovir, IIIvir/triumvir, IIIIIIvir/sevir ...
    "vir", "viro", "viri", "viris", "virum", "virorum", "virei", "viratus",
    # oared warships named for their banks of oars
    "triere", "trieris", "triremi", "triremes", "quadriere", "pentere", "hexere",
}

# The Latin reading of a Type 3 compound. Recorded in normalized_form only,
# never as a gold label. [VERIFY -- LATINIST]
TYPE3_NORMALISED = {
    "II": "duo", "III": "tri", "IIII": "quattuor", "IV": "quattuor",
    "V": "quinque", "VI": "se", "IIIIII": "se", "VII": "septem",
    "VIII": "octo", "X": "decem", "XV": "quindecim", "XX": "viginti",
    "IIIII": "quinque",
}
TYPE3_OFFICE_READING = {
    ("II", "vir"): "duumvir", ("III", "vir"): "triumvir",
    ("IIII", "vir"): "quattuorvir", ("IV", "vir"): "quattuorvir",
    ("V", "vir"): "quinquevir", ("VI", "vir"): "sevir",
    ("IIIIII", "vir"): "sevir", ("VII", "vir"): "septemvir",
    ("X", "vir"): "decemvir", ("XV", "vir"): "quindecimvir",
    ("XX", "vir"): "vigintivir", ("XXVI", "vir"): "vigintisexvir",
    ("III", "triere"): "trieris", ("IIII", "quadriere"): "quadrieris",
    ("IIIII", "pentere"): "penteris", ("VI", "hexere"): "hexeris",
}

if __name__ == "__main__":
    for name, s in (("TYPE1_NUMERAL_WORD", TYPE1_NUMERAL_WORD),
                    ("TYPE2_SUPPLIED_UNIT", TYPE2_SUPPLIED_UNIT),
                    ("TYPE3_NUMERAL_PREFIX", TYPE3_NUMERAL_PREFIX)):
        print(f"\n{name}  ({len(s)} entries)")
        for w in sorted(s):
            print(f"    {w}")
    overlap = (TYPE1_NUMERAL_WORD & TYPE2_SUPPLIED_UNIT) | \
              (TYPE1_NUMERAL_WORD & TYPE3_NUMERAL_PREFIX) | \
              (TYPE2_SUPPLIED_UNIT & TYPE3_NUMERAL_PREFIX)
    print(f"\ncross-list overlap (must be empty): {sorted(overlap) or 'none'}")
