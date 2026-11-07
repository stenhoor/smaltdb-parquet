/**
 * @smaltdb/parquet: the Parquet format for smaltdb external tables and `read_parquet()` (ADR 0012).
 *
 * Reads only what a statement needs: the footer first (the last 16 KB, more if the metadata is
 * longer), then only the column chunks of the columns the statement uses, in the row groups whose
 * statistics do not rule them out. Files under 64 KB are read in one request.
 *
 * Decoding is done by hyparquet; codecs other than Snappy and uncompressed need
 * hyparquet-compressors, loaded on first use.
 */
import type * as Hyparquet from 'hyparquet';
import type { FormatColumn, FormatRequest, FormatResult, RandomAccessFile, SmaltPlugin, SqlValue, Zone } from 'smaltdb/engine';

declare const __PLUGIN_VERSION__: string | undefined;
const VERSION = typeof __PLUGIN_VERSION__ === 'string' ? __PLUGIN_VERSION__ : '0.0.0';

const parquet: SmaltPlugin = {
  name: '@smaltdb/parquet',
  version: VERSION,
  apiVersion: 1,
  setup(host) {
    host.addFormat({ name: 'parquet', aliases: ['pq'], extensions: ['parquet', 'pq'], access: 'random', pushdown: true, read: readParquet });
    host.addTableFunction('read_parquet', { format: 'parquet' });
  },
};
export default parquet;

let compressorsModule: Promise<{ compressors: Hyparquet.Compressors }> | null = null;

/** hyparquet-compressors (gzip, zstd, brotli, lz4), loaded only for files that need it. */
async function compressors(): Promise<Hyparquet.Compressors> {
  compressorsModule ??= import('hyparquet-compressors');
  try {
    return (await compressorsModule).compressors;
  } catch {
    compressorsModule = null;
    throw new Error('this file uses a codec that needs the hyparquet-compressors package (npm install hyparquet-compressors)');
  }
}

async function readParquet(file: RandomAccessFile, req: FormatRequest): Promise<FormatResult> {
  const pq = await import('hyparquet');
  // Files under 64 KB are read in one request (fewer round trips); larger ones by ranges.
  const buffer = pq.cachedAsyncBuffer(file as Hyparquet.AsyncBuffer, { minSize: 1 << 16 });
  let meta: Hyparquet.FileMetaData;
  try {
    // The footer is usually a few KB: start with 16 KB from the end (hyparquet fetches more when
    // the metadata is longer), not its default 512 KB, which is all of a small file.
    meta = await pq.parquetMetadataAsync(buffer, { parsers: PARQUET_PARSERS, initialFetchSize: 1 << 14 } as Hyparquet.MetadataOptions);
  } catch (e) {
    // No footer where the size puts it: maybe the size is wrong (a host that reported a compressed
    // size), so the engine may retry with the whole file.
    throw Object.assign(new Error(`not a readable Parquet file: ${(e as Error).message}`), { wholeFileMayHelp: true });
  }
  const schema = pq.parquetSchema(meta).children;
  const fileCols: FormatColumn[] = schema.map((c) => {
    const type = parquetTemporalType(c);
    return { name: c.element.name, declType: parquetDeclType(c), ...(type ? { type } : {}) };
  });
  if (req.schemaOnly) return { columns: fileCols, rows: [] };
  const out = req.columns ?? fileCols;
  const actual = new Map(fileCols.map((c) => [c.name.toLowerCase(), c.name]));
  // Columns to read: the projection's (or all), that exist in this file.
  const want = out.filter((c) => actual.has(c.name.toLowerCase()) && (req.projection === undefined || req.projection.has(c.name.toLowerCase())));
  const at = new Map(want.map((c, i) => [c.name.toLowerCase(), i]));
  const pos = out.map((c) => at.get(c.name.toLowerCase()) ?? -1);
  // Row groups whose statistics cannot rule them out, as row ranges.
  const ranges: [number, number][] = [];
  let start = 0;
  for (const rg of meta.row_groups) {
    const n = Number(rg.num_rows);
    const keep = !req.blockFilters?.length || req.blockFilters.every((p) => p(rowGroupZones(rg), n) !== false);
    if (keep && n > 0) {
      const last = ranges[ranges.length - 1];
      if (last && last[1] === start) last[1] = start + n;
      else ranges.push([start, start + n]);
    }
    start += n;
  }
  const codecs = new Set(meta.row_groups.flatMap((rg) => rg.columns.map((c) => c.meta_data?.codec)));
  const extra = [...codecs].some((c) => c && c !== 'UNCOMPRESSED' && c !== 'SNAPPY') ? await compressors() : undefined;
  const rows: SqlValue[][] = [];
  for (const [rowStart, rowEnd] of ranges) {
    if (want.length === 0) {
      // No column is read (count(*)): only the number of rows matters.
      for (let i = rowStart; i < rowEnd; i++) rows.push(out.map(() => null));
      continue;
    }
    let chunk: unknown[][] = [];
    try {
      await pq.parquetRead({
        file: buffer,
        metadata: meta,
        columns: want.map((c) => actual.get(c.name.toLowerCase())!),
        rowStart,
        rowEnd,
        rowFormat: 'array',
        utf8: false,
        parsers: PARQUET_PARSERS,
        compressors: extra,
        onComplete: (r) => (chunk = r),
      });
    } catch (e) {
      throw new Error(`Parquet: ${(e as Error).message}`);
    }
    for (const r of chunk) rows.push(pos.map((i) => (i < 0 ? null : parquetValue(r[i]))));
  }
  return { columns: out, rows };
}

/** µs as smaltdb holds them: a number within 2^53, else a bigint. */
const asUs = (us: bigint): number | bigint => (us >= MIN_SAFE && us <= MAX_SAFE ? Number(us) : us);

/** `v` divided by `d`, rounded toward negative infinity (nanoseconds are cut to µs: smaltdb ADR 0019 decision 1). */
const floorDiv = (v: bigint, d: bigint): bigint => (v % d < 0n ? v / d - 1n : v / d);

/**
 * Dates and timestamps as smaltdb's `date` and `datetime` values (ADR 0019): days since 1970-01-01, and µs since
 * 1970-01-01 00:00:00. Statistics read through the same parsers, so zone maps compare the same numbers.
 */
const PARQUET_PARSERS: Partial<Hyparquet.ParquetParsers> = {
  dateFromDays: (days: number) => days,
  timestampFromMilliseconds: (ms: bigint) => asUs(BigInt(ms) * 1000n),
  timestampFromMicroseconds: (us: bigint) => asUs(BigInt(us)),
  timestampFromNanoseconds: (ns: bigint) => asUs(floorDiv(BigInt(ns), 1000n)),
} as unknown as Partial<Hyparquet.ParquetParsers>;

/** The temporal type of a top-level Parquet column, if it has one. */
function parquetTemporalType(c: Hyparquet.SchemaTree): 'date' | 'datetime' | undefined {
  const e = c.element;
  if (c.children.length > 0) return undefined;
  const ct = e.converted_type;
  const lt = e.logical_type?.type;
  if (ct === 'DATE' || lt === 'DATE') return 'date';
  if (ct === 'TIMESTAMP_MILLIS' || ct === 'TIMESTAMP_MICROS' || lt === 'TIMESTAMP' || e.type === 'INT96') return 'datetime';
  return undefined;
}

/** The declared type of a top-level Parquet column (nested ones are JSON text). */
function parquetDeclType(c: Hyparquet.SchemaTree): string {
  const e = c.element;
  if (c.children.length > 0) return 'TEXT';
  const ct = e.converted_type;
  const lt = e.logical_type?.type;
  if (ct === 'DECIMAL' || lt === 'DECIMAL' || lt === 'FLOAT16') return 'REAL';
  if (ct === 'DATE' || lt === 'DATE') return 'DATE';
  if (ct === 'TIMESTAMP_MILLIS' || ct === 'TIMESTAMP_MICROS' || lt === 'TIMESTAMP' || e.type === 'INT96') return 'TIMESTAMP';
  if (ct === 'UTF8' || ct === 'ENUM' || ct === 'JSON' || lt === 'STRING' || lt === 'ENUM' || lt === 'JSON' || lt === 'UUID') return 'TEXT';
  switch (e.type) {
    case 'BOOLEAN':
    case 'INT32':
    case 'INT64':
      return 'INTEGER';
    case 'FLOAT':
    case 'DOUBLE':
      return 'REAL';
    default:
      return 'BLOB';
  }
}

const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER);
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

/** A value hyparquet decoded, as a SQL value (nested values as JSON text). */
function parquetValue(v: unknown): SqlValue {
  if (v === null || v === undefined) return null;
  if (typeof v === 'number') return Number.isNaN(v) ? null : v;
  if (typeof v === 'bigint') return v >= MIN_SAFE && v <= MAX_SAFE ? Number(v) : v;
  if (typeof v === 'boolean') return v ? 1 : 0;
  if (typeof v === 'string' || v instanceof Uint8Array) return v;
  return JSON.stringify(v, (_k, x) => (typeof x === 'bigint' ? (x >= MIN_SAFE && x <= MAX_SAFE ? Number(x) : x.toString()) : x instanceof Uint8Array ? Array.from(x) : x));
}

/** Zone maps of a row group's top-level columns from their statistics (lower-cased names). */
function rowGroupZones(rg: Hyparquet.RowGroup): Map<string, Zone> {
  const zones = new Map<string, Zone>();
  for (const c of rg.columns) {
    const md = c.meta_data;
    if (!md || md.path_in_schema.length !== 1 || !md.statistics) continue;
    const st = md.statistics;
    const z: Zone = { nulls: st.null_count === undefined ? -1 : Number(st.null_count) };
    // min_value / max_value are ordered correctly for every type; the deprecated min / max only for numbers.
    const num = (x: unknown): number | undefined =>
      typeof x === 'number' && !Number.isNaN(x) ? x : typeof x === 'bigint' && x >= MIN_SAFE && x <= MAX_SAFE ? Number(x) : typeof x === 'boolean' ? (x ? 1 : 0) : undefined;
    const lo = st.min_value ?? (md.type === 'BYTE_ARRAY' || md.type === 'FIXED_LEN_BYTE_ARRAY' ? undefined : st.min);
    const hi = st.max_value ?? (md.type === 'BYTE_ARRAY' || md.type === 'FIXED_LEN_BYTE_ARRAY' ? undefined : st.max);
    const nlo = num(lo);
    const nhi = num(hi);
    if (nlo !== undefined && nhi !== undefined) {
      z.numMin = nlo;
      z.numMax = nhi;
    } else if (typeof lo === 'string' && typeof hi === 'string') {
      z.textMin = lo;
      z.textMax = hi;
    }
    zones.set(md.path_in_schema[0].toLowerCase(), z);
  }
  return zones;
}
