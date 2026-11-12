/**
 * smaltql's documentation examples that need Parquet (the SQL guide and references moved from smaltdb to smaltql, smaltdb
 * ADR 0015). smaltql's docs (docs/guide, docs/reference) mark them with `<!-- docs:plugin @smaltdb/parquet -->`, and
 * smaltql's own docs test skips them. Here every page with such a block runs top to bottom with all three plugins
 * installed, as smaltql's harness does, against the docs at the smaltql commit pinned in packed.json (kept by
 * scripts/fetch-packed.mjs). TypeScript examples on those pages are type-checked against the pinned smaltdb and smaltql,
 * the installed plugins and this plugin's source. The harness and its markers are described in smaltql's
 * test/docs-examples.test.ts.
 */
import { execFileSync } from 'node:child_process';
import { mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { join, relative } from 'node:path';
import { describe, expect, it } from 'vitest';
import external from '@smaltdb/external';
import { registerFile } from '@smaltdb/external/client';
import io from '@smaltdb/io';
import { Database, type SqlValue } from 'smaltdb/engine';
import { parse } from '@smaltworks/smaltql';
import parquet from '../src/index.js';

const pins = JSON.parse(readFileSync('packed.json', 'utf8')) as Record<string, { ref: string }>;
const DOCS = join('.cache', 'packed', `smaltworks-smaltql-${pins['@smaltworks/smaltql']!.ref}-files`, 'docs');
const PAGE_DIRS = ['guide', 'reference'];
const EXAMPLES = join(DOCS, 'examples');
const MARKER = /<!--\s*docs:plugin\s+@smaltdb\/parquet\s*-->/;

function pages(): string[] {
  const out: string[] = [];
  const walk = (dir: string): void => {
    for (const e of readdirSync(dir).sort()) {
      const p = join(dir, e);
      if (statSync(p).isDirectory()) walk(p);
      else if (e.endsWith('.md')) out.push(p);
    }
  };
  for (const d of PAGE_DIRS) {
    try {
      walk(join(DOCS, d));
    } catch {
      /* directory not there yet */
    }
  }
  // Only the pages with a block for this plugin.
  return out.filter((p) => MARKER.test(readFileSync(p, 'utf8')));
}

interface Block {
  lang: string;
  code: string;
  line: number;
  markers: string[];
  table: { header: string[]; rows: string[][] } | null;
}

function splitRow(line: string): string[] {
  const s = line.trim().replace(/^\|/, '').replace(/\|$/, '');
  const cells: string[] = [];
  let cur = '';
  for (let i = 0; i < s.length; i++) {
    if (s[i] === '\\' && s[i + 1] === '|') {
      cur += '|';
      i++;
    } else if (s[i] === '|') {
      cells.push(cur);
      cur = '';
    } else cur += s[i];
  }
  cells.push(cur);
  return cells.map((c) => {
    const t = c.trim();
    const m = /^`(.*)`$/.exec(t);
    if (!m) return t;
    const inner = m[1];
    return /^'.*'$/.test(inner) ? inner.slice(1, -1) : inner;
  });
}

function blocks(md: string): Block[] {
  const lines = md.split('\n');
  const out: Block[] = [];
  let markers: string[] = [];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const marker = /^\s*<!--\s*(docs:[\s\S]*?)\s*-->\s*$/.exec(line);
    if (marker) {
      markers.push(marker[1]);
      continue;
    }
    const fence = /^(\s*)(```+)\s*([\w-]*)/.exec(line);
    if (!fence) {
      if (line.trim() !== '') markers = [];
      continue;
    }
    const start = i;
    const body: string[] = [];
    for (i++; i < lines.length && !lines[i].trim().startsWith(fence[2]); i++) body.push(lines[i]);
    // A result table may follow after blank lines.
    let j = i + 1;
    while (j < lines.length && lines[j].trim() === '') j++;
    let table: Block['table'] = null;
    if (fence[3] === 'sql' && lines[j]?.trim().startsWith('|') && /^\s*\|?\s*:?-+/.test(lines[j + 1] ?? '')) {
      const header = splitRow(lines[j]);
      const rows: string[][] = [];
      let k = j + 2;
      for (; k < lines.length && lines[k].trim().startsWith('|'); k++) rows.push(splitRow(lines[k]));
      table = { header, rows };
      i = k - 1;
    }
    out.push({ lang: fence[3], code: body.join('\n'), line: start + 1, markers, table });
    markers = [];
  }
  return out;
}

function show(v: SqlValue): string {
  if (v === null) return 'NULL';
  if (v instanceof Uint8Array) return `x'${Array.from(v, (b) => b.toString(16).padStart(2, '0')).join('')}'`;
  return String(v);
}

async function registerExamples(db: Database): Promise<void> {
  let names: string[] = [];
  try {
    names = readdirSync(EXAMPLES);
  } catch {
    return;
  }
  for (const n of names) {
    const p = join(EXAMPLES, n);
    if (statSync(p).isFile()) await registerFile(db, n, new Uint8Array(readFileSync(p)));
  }
}

async function runSql(db: Database, sql: string): Promise<{ columns: string[]; rows: SqlValue[][] }> {
  const stmts = parse(sql);
  for (let i = 0; i < stmts.length - 1; i++) await db.execAsync(stmts[i].text);
  return db.queryAsync(stmts[stmts.length - 1].text);
}

const all = pages();

describe('documentation SQL examples', () => {
  it('finds the pages with Parquet examples', () => {
    expect(all.length).toBeGreaterThan(0);
  });
  for (const page of all) {
    it(page, async () => {
      // Every plugin, as an application that uses them would.
      const db = Database.openMemory({ plugins: [external, io, parquet] });
      await registerExamples(db);
      try {
        for (const b of blocks(readFileSync(page, 'utf8'))) {
          const where = `${page}:${b.line}`;
          for (const m of b.markers) {
            const setup = /^docs:setup\s+([\s\S]*)$/.exec(m);
            if (setup) await db.execAsync(setup[1]);
          }
          if (b.lang !== 'sql' || b.markers.includes('docs:skip')) continue;
          const err = b.markers.map((m) => /^docs:error\s+(\S+)/.exec(m)?.[1]).find(Boolean);
          if (err) {
            // A syntax error anywhere in the block fails parse(): run the block as one script then.
            let stmts: { text: string }[];
            try {
              stmts = parse(b.code);
            } catch {
              stmts = [{ text: b.code }];
            }
            for (let i = 0; i < stmts.length - 1; i++) await db.execAsync(stmts[i].text);
            const got = await db.execAsync(stmts[stmts.length - 1].text).then(
              () => 'no error',
              (e: { code?: string; message: string }) => e.code ?? e.message,
            );
            expect(got, `${where}: expected error ${err}`).toBe(err);
            continue;
          }
          let result: { columns: string[]; rows: SqlValue[][] };
          try {
            result = await runSql(db, b.code);
          } catch (e) {
            throw new Error(`${where}: ${(e as Error).message}\n${b.code}`);
          }
          if (!b.table) continue;
          expect(result.columns, `${where}: columns`).toEqual(b.table.header);
          const got = result.rows.map((r) => r.map(show));
          const want = b.table.rows;
          if (b.markers.includes('docs:unordered')) {
            const key = (r: string[]): string => JSON.stringify(r);
            expect(got.map(key).sort(), `${where}: rows`).toEqual(want.map(key).sort());
          } else expect(got, `${where}: rows`).toEqual(want);
        }
      } finally {
        db.close();
      }
    });
  }
});

describe('documentation TypeScript examples', () => {
  it('type-check against the public API', () => {
    const dir = join('.cache', 'docs-ts');
    rmSync(dir, { recursive: true, force: true });
    mkdirSync(dir, { recursive: true });
    const files: string[] = [];
    const origin = new Map<string, string>();
    for (const page of all) {
      blocks(readFileSync(page, 'utf8')).forEach((b, n) => {
        if (!['ts', 'typescript'].includes(b.lang) || b.markers.includes('docs:skip')) return;
        // `db` (an open client database) is predeclared unless the block makes its own.
        const declaresDb = /\b(const|let|var)\s+db\b|\(\s*db\s*(:[^)]*)?\)\s*=>|\bfunction\s*\w*\s*\(\s*db\b/.test(b.code);
        const prelude = declaresDb ? '' : "declare const db: import('smaltdb').Database;\n";
        const name = `${page.replace(/[\\/]/g, '_').replace(/\.md$/, '')}_${n}.ts`;
        writeFileSync(join(dir, name), `${prelude}${b.code}\nexport {};\n`);
        files.push(name);
        origin.set(name, `${page}:${b.line}`);
      });
    }
    if (files.length === 0) return;
    const root = relative(dir, '.');
    writeFileSync(
      join(dir, 'tsconfig.json'),
      JSON.stringify({
        compilerOptions: {
          target: 'ES2022',
          module: 'ESNext',
          moduleResolution: 'Bundler',
          lib: ['ES2022', 'DOM', 'WebWorker'],
          strict: true,
          noEmit: true,
          skipLibCheck: true,
          types: ['node'],
          typeRoots: [join(root, 'node_modules/@types')],
          paths: {
            smaltdb: [join(root, 'node_modules/smaltdb/src/client/index.ts')],
            'smaltdb/engine': [join(root, 'node_modules/smaltdb/src/engine/index.ts')],
            'smaltdb/worker-runtime': [join(root, 'node_modules/smaltdb/src/engine/worker-runtime.ts')],
            '@smaltdb/parquet': [join(root, 'src/index.ts')],
          },
        },
        files,
      }),
    );
    let output = '';
    try {
      execFileSync(process.execPath, [join('node_modules', 'typescript7', 'bin', 'tsc'), '-p', join(dir, 'tsconfig.json')], { encoding: 'utf8' });
    } catch (e) {
      output = String((e as { stdout?: string }).stdout ?? e);
    }
    // Name the documentation page and line of each failing block.
    const report = output.replace(/^\.cache[\\/]docs-ts[\\/](\S+?\.ts)/gm, (_m, f: string) => `${origin.get(f) ?? f} (${f})`);
    expect(report, report).toBe('');
  }, 120_000);
});
