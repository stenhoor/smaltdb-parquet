// Browser test (Playwright): `npm run test:browser`. Uses a globally installed Playwright (see playwright.mjs);
// BROWSERS=chromium,firefox,webkit picks the browsers (default chromium).
import { build } from 'esbuild';
import { createServer } from 'node:http';
import { copyFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { loadPlaywright } from './playwright.mjs';
import { compressingHandler } from './compressing.mjs';
import { serve } from './serve.mjs';

const playwright = loadPlaywright();
const out = '.cache/browser-test';
mkdirSync(out, { recursive: true });
// smaltdb and @smaltdb/external resolve to their installed packages (scripts/fetch-packed.mjs), as in an application.
for (const name of ['page', 'worker']) {
  await build({ entryPoints: [`test/browser/${name}.ts`], bundle: true, format: 'esm', target: 'es2022', outfile: `${out}/${name}.js`, logLevel: 'warning' });
}
// Parquet files written by pyarrow, read with Range requests.
copyFileSync('test/fixtures/parquet/sales-zstd.parquet', `${out}/sales.parquet`);
copyFileSync('test/fixtures/parquet/types.parquet', `${out}/types.parquet`);
writeFileSync(`${out}/index.html`, '<!doctype html><meta charset="utf-8"><title>@smaltdb/parquet</title><script type="module" src="./page.js"></script>');

// Hosts that compress responses (smaltdb issue #32): one under /gz/ on the page's origin, and one on another origin
// whose CORS headers expose only Content-Length and ETag.
const gzLog = [];
const server = await serve({ '/': out }, { handle: compressingHandler('/gz/', out, { log: gzLog }) });
const xoLog = [];
const xoHandle = compressingHandler('/gz/', out, { log: xoLog, cors: server.url });
const xoServer = createServer(async (req, res) => {
  if (!(await xoHandle(req, res))) res.writeHead(404).end();
});
await new Promise((r) => xoServer.listen(0, '127.0.0.1', r));
const xoUrl = `http://127.0.0.1:${xoServer.address().port}`;

let failed = false;
for (const name of (process.env.BROWSERS ?? 'chromium').split(',')) {
  const browser = await playwright[name].launch();
  const page = await browser.newPage();
  const log = [];
  page.on('console', (m) => log.push(`console.${m.type()}: ${m.text()}`));
  page.on('pageerror', (e) => log.push(`pageerror: ${e.stack ?? e.message}`));
  page.on('requestfailed', (r) => log.push(`request failed: ${r.url()} ${r.failure()?.errorText ?? ''}`));
  gzLog.length = 0;
  xoLog.length = 0;
  await page.goto(server.url + '/?xo=' + encodeURIComponent(xoUrl));
  let result;
  try {
    await page.waitForFunction(() => window.__result !== undefined, null, { timeout: 60_000 });
    result = await page.evaluate(() => window.__result);
  } catch (e) {
    result = { ok: false, error: `no result: ${e.message.split('\n')[0]}` };
  }
  if (result.ok) {
    // What the compressing hosts saw: same-origin, a range probe and ranged reads only (nothing gzipped);
    // cross-origin, where Content-Range is hidden, the probe and then whole-file GETs.
    const same = gzLog.filter((r) => r.path === '/gz/sales.parquet');
    const cross = xoLog.filter((r) => r.path === '/gz/sales.parquet');
    result.checks.push({
      name: 'compressing hosts: same-origin reads by ranges after a range probe',
      ok: same.length > 0 && same[0].range === 'bytes=0-0' && same.every((r) => r.method === 'GET' && r.range !== null),
      detail: JSON.stringify(same),
    });
    result.checks.push({
      name: 'compressing hosts: cross-origin falls back to whole-file reads',
      ok: cross.length > 0 && cross[0].range === 'bytes=0-0' && cross.some((r) => r.method === 'GET' && r.range === null),
      detail: JSON.stringify(cross),
    });
  }
  console.log(`\n== ${name} ${browser.version()}`);
  await browser.close();
  if (!result.ok) {
    console.log('FAIL  ' + result.error);
    for (const l of log) console.log('  ' + l);
    failed = true;
    continue;
  }
  if (result.checks.some((c) => !c.ok)) for (const l of log) console.log('  ' + l);
  for (const c of result.checks) {
    console.log(`${c.ok ? 'PASS' : 'FAIL'}  ${c.name}${c.detail && !c.ok ? `  (${c.detail})` : ''}`);
    if (!c.ok) failed = true;
  }
}
server.close();
xoServer.close();
process.exit(failed ? 1 : 0);
