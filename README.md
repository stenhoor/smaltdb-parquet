# @smaltdb/parquet

<a href="https://smaltworks.dev/stability"><picture><source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/stenhoor/smalt-brand/main/badges/work-in-progress-on-dark.svg"><img alt="Work in progress. Come build with us." src="https://raw.githubusercontent.com/stenhoor/smalt-brand/main/badges/work-in-progress.svg" height="56"></picture></a>

[Writing plugins](https://smaltdb.dev/reference/api/plugins) · [smaltworks.dev](https://smaltworks.dev/products/plugins)

The Parquet format for [smaltdb](https://github.com/stenhoor/smaltdb): `read_parquet()` and external tables over
`.parquet` files. Files are decoded with [hyparquet](https://github.com/hyparam/hyparquet). Only the footer, the
columns a statement uses and the row groups its filters cannot rule out are read.

```sh
npm install smaltdb @smaltdb/parquet
```

Snappy, gzip, zstd, Brotli and LZ4 files all read. The codecs other than Snappy come from
[hyparquet-compressors](https://github.com/hyparam/hyparquet-compressors), which the plugin brings with it and loads
only for files that use them.

In the browser, install it in your engine worker entry and open the database with that worker:

```ts
// db-worker.ts
import { serve } from 'smaltdb/worker-runtime';
import parquet from '@smaltdb/parquet';

serve({ plugins: [parquet] });
```

```ts
import { open } from 'smaltdb';

const db = await open('app', {
  worker: () => new Worker(new URL('./db-worker.ts', import.meta.url), { type: 'module' }),
});
const rows = await db.query("SELECT count(*) AS n FROM read_parquet('https://example.com/trips.parquet')");
```

With the in-process engine (Node): `Database.openMemory({ plugins: [parquet] })` from `smaltdb/engine`.

See the [plugins guide](https://github.com/stenhoor/smaltdb/blob/main/docs/guide/plugins.md). MIT licence.

## Development

```sh
npm ci && npm run deps   # deps: the pinned smaltdb, @smaltdb/io and @smaltdb/external builds from packed.json
npm run check            # type-check, unit tests and smaltql's Parquet docs examples
npm run test:browser     # Parquet over HTTP in a browser (Playwright)
```

The fixtures in `test/fixtures/parquet` are written by pyarrow: `pip install pyarrow && python3 scripts/gen-parquet-fixtures.py`.
See [CLAUDE.md](CLAUDE.md) for how the pins work.

<a href="https://smaltworks.dev"><picture><source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/stenhoor/smalt-brand/main/badges/a-smaltworks-project-on-dark.svg"><img alt="A smaltworks project" src="https://raw.githubusercontent.com/stenhoor/smalt-brand/main/badges/a-smaltworks-project.svg" height="28"></picture></a>

MIT License.
