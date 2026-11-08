// Runs on the page: reads Parquet over HTTP through the real engine worker, from a plain host and from hosts that
// compress responses (smaltdb issue #32). Reports { ok, checks } on window.__result.
import { open } from 'smaltdb';

type Check = { name: string; ok: boolean; detail?: string };

async function run(): Promise<Check[]> {
  const checks: Check[] = [];
  const check = (name: string, ok: boolean, detail?: unknown) => checks.push({ name, ok, detail: detail === undefined ? undefined : JSON.stringify(detail) });
  const db = await open('parquet-test', { storage: 'memory', worker: () => new Worker(new URL('./worker.js', location.href), { type: 'module' }) });
  try {
    // Footer, column chunks and row groups by ranges; zstd through hyparquet-compressors.
    const pq = await db.query(`SELECT count(*), sum(amount), min(day), max(id) FROM read_parquet('${location.origin}/sales.parquet') WHERE id >= 1000`, undefined, { rowMode: 'array' });
    const pqTypes = await db.query(`SELECT i64, d, ts, dec, lst FROM read_parquet('${location.origin}/types.parquet') WHERE i32 = 1`, undefined, { rowMode: 'array' });
    check('Parquet over HTTP (zstd, row-group pruning, types)', JSON.stringify(pq) === JSON.stringify([[3000, 144154, '2024-01-21', 3999]]) && JSON.stringify(pqTypes) === JSON.stringify([[2 ** 40, '2024-02-29', '2024-01-02 03:04:05.678', 12.34, '[1,2]']]), [pq, pqTypes]);

    // Hosts that compress responses (GitHub Pages): HEAD's Content-Length is the gzipped size there. Same-origin, and
    // cross-origin with only Content-Length and ETag exposed by CORS.
    const xo = new URLSearchParams(location.search).get('xo');
    for (const [name, base] of [
      ['same-origin', `${location.origin}/gz`],
      ['cross-origin, Content-Range not exposed', `${xo}/gz`],
    ]) {
      const got = await db
        .query(`SELECT count(*), sum(amount), min(day), max(id) FROM read_parquet('${base}/sales.parquet') WHERE id >= 1000`, undefined, { rowMode: 'array' })
        .then((r) => JSON.stringify(r), (e: Error) => e.message);
      await db.exec(`DROP EXTERNAL TABLE IF EXISTS gz; CREATE EXTERNAL TABLE gz WITH (location = '${base}/sales.parquet')`).catch(() => undefined);
      const ext = await db.query('SELECT count(*), count(DISTINCT region) FROM gz', undefined, { rowMode: 'array' }).then((r) => JSON.stringify(r), (e: Error) => e.message);
      await db.exec('DROP EXTERNAL TABLE IF EXISTS gz');
      check(`Parquet from a host that compresses responses (${name})`, got === JSON.stringify([[3000, 144154, '2024-01-21', 3999]]) && ext === JSON.stringify([[4000, 3]]), [got, ext]);
    }
  } finally {
    await db.close();
  }
  return checks;
}

run().then(
  (checks) => ((window as unknown as { __result: unknown }).__result = { ok: true, checks }),
  (e: Error) => ((window as unknown as { __result: unknown }).__result = { ok: false, error: e.stack ?? e.message }),
);
