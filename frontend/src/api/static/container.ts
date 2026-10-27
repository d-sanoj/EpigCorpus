/* The typed-array container the static build serves (data/corpus.bin.gz).
 *
 * Written by backend/src/epigcorpus/export/static_site.py; the two must stay in
 * step. Layout: a u32 header length, that much JSON, then the arrays, each
 * 8-byte aligned with its byte offset recorded in the header, so the reader
 * never guesses a layout.
 *
 * The dictionary columns reserve index 0. Places put "?" there, the name the
 * map itself gives a findspot the corpus does not name; provinces and
 * materials put "" there, and the filter options exclude it, which is what
 * makes a row without one unselectable - exactly how `isin` behaves on the
 * server.
 *
 * Rows are the API's search frame, in its order: the rows the server would
 * search, minus those without coordinates or outside the Roman bounds.
 */
export const CONTAINER_FORMAT = "epigcorpus-static-1";

/** A year the corpus lacks. The API answers null for it. */
export const MISSING_YEAR = -2147483648;

/** The three texts a query can search: the API's search_in values. */
export type Mode = "raw" | "conservative" | "interpretive";

export const MODES: Mode[] = ["raw", "conservative", "interpretive"];

/** The corpus column a mode searches, as the API names it. */
export const MODE_FIELDS: Record<Mode, "inscription_text" | "inscription_text_conservative" | "inscription_text_interpretive"> = {
  raw: "inscription_text",
  conservative: "inscription_text_conservative",
  interpretive: "inscription_text_interpretive",
};

export interface Corpus {
  fingerprint: string;
  rows: number;
  /** Index 0 is "?": a place the corpus does not name. */
  places: string[];
  /** Index 0 is "": a missing value, never matched by a filter. */
  provinces: string[];
  materials: string[];
  /** The tokens a language filter matches. */
  languages: string[];
  /** The corpus's own string ("Greek, Latin"), which the table prints. */
  languageText: string[];
  /** Category values in the order each row carries them. */
  categories: string[];
  place: Uint32Array;
  /** Index into the coordinate pairs; 5-decimal pairs, as the map groups them. */
  coord: Uint32Array;
  province: Uint16Array;
  material: Uint16Array;
  /** EDCS-00000001-0 as 1 and 0. */
  edcs: Uint32Array;
  index: Uint16Array;
  notBefore: Int32Array;
  notAfter: Int32Array;
  /** One flat list of token ids plus row offsets. */
  languageOffsets: Uint32Array;
  languageIds: Uint16Array;
  languageTextCode: Uint16Array;
  categoryOffsets: Uint32Array;
  categoryIds: Uint16Array;
  coordLatitude: Float64Array;
  coordLongitude: Float64Array;
  /** Where each row's text starts: rows + 1 offsets per mode. */
  textOffsets: Record<Mode, Uint32Array>;
}

interface ArraySpec {
  dtype: keyof typeof CONSTRUCTORS;
  offset: number;
  length: number;
}

interface Header {
  format: string;
  fingerprint: string;
  rows: number;
  modes: Mode[];
  coord_count: number;
  dicts: {
    place: string[];
    province: string[];
    material: string[];
    language: string[];
    language_text: string[];
    category: string[];
  };
  arrays: Record<string, ArraySpec>;
}

const CONSTRUCTORS = {
  u16: Uint16Array,
  u32: Uint32Array,
  i32: Int32Array,
  f64: Float64Array,
} as const;

type View = new (buffer: ArrayBuffer, byteOffset: number, length: number) => ArrayBufferView;

export function decodeContainer(buffer: ArrayBuffer): Corpus {
  const headerLength = new DataView(buffer).getUint32(0, true);
  const header = JSON.parse(
    new TextDecoder().decode(new Uint8Array(buffer, 4, headerLength)),
  ) as Header;
  if (header.format !== CONTAINER_FORMAT) {
    throw new Error(`This page reads ${CONTAINER_FORMAT}, not ${header.format}.`);
  }

  const payload = 4 + headerLength;
  function array<T>(name: string): T {
    const spec = header.arrays[name];
    if (!spec) throw new Error(`The corpus container has no ${name} array.`);
    const start = payload + spec.offset;
    // f64 needs eight, the rest four: the writer aligns every array, so a
    // misaligned one means the file is not what this reader expects.
    if (start % 8 !== 0) throw new Error(`The ${name} array is misaligned.`);
    const View = CONSTRUCTORS[spec.dtype] as unknown as View;
    return new View(buffer, start, spec.length) as unknown as T;
  }

  const textOffsets = {} as Record<Mode, Uint32Array>;
  for (const mode of header.modes) textOffsets[mode] = array<Uint32Array>(`text_${mode}`);

  const corpus: Corpus = {
    fingerprint: header.fingerprint,
    rows: header.rows,
    places: header.dicts.place,
    provinces: header.dicts.province,
    materials: header.dicts.material,
    languages: header.dicts.language,
    languageText: header.dicts.language_text,
    categories: header.dicts.category,
    place: array<Uint32Array>("place"),
    coord: array<Uint32Array>("coord"),
    province: array<Uint16Array>("province"),
    material: array<Uint16Array>("material"),
    edcs: array<Uint32Array>("edcs"),
    index: array<Uint16Array>("index"),
    notBefore: array<Int32Array>("not_before"),
    notAfter: array<Int32Array>("not_after"),
    languageOffsets: array<Uint32Array>("language_offsets"),
    languageIds: array<Uint16Array>("language_ids"),
    languageTextCode: array<Uint16Array>("language_text"),
    categoryOffsets: array<Uint32Array>("category_offsets"),
    categoryIds: array<Uint16Array>("category_ids"),
    coordLatitude: array<Float64Array>("coord_latitude"),
    coordLongitude: array<Float64Array>("coord_longitude"),
    textOffsets,
  };

  // A truncated or half-updated file would otherwise read as plausible rows.
  if (corpus.place.length !== corpus.rows) throw new Error("The corpus container is truncated.");
  if (corpus.coordLatitude.length !== header.coord_count) {
    throw new Error("The corpus container is truncated.");
  }
  for (const mode of MODES) {
    const offsets = corpus.textOffsets[mode];
    if (!offsets || offsets.length !== corpus.rows + 1) {
      throw new Error(`The corpus container has no ${mode} text offsets.`);
    }
  }
  return corpus;
}
