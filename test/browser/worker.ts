// The engine worker entry, as an application writes it (smaltdb ADR 0012, 0015): SQL from smaltql, and Parquet over
// sources from @smaltdb/external.
import { defineWorker } from 'smaltdb/worker-runtime';
import { sqlLanguage } from '@smaltworks/smaltql';
import external from '@smaltdb/external';
import parquet from '../../src/index.js';

defineWorker({ languages: [sqlLanguage], plugins: [external, parquet] });
