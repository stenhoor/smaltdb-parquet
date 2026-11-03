// The engine worker entry, as an application writes it (smaltdb ADR 0012): Parquet over sources from @smaltdb/external.
import { serve } from 'smaltdb/worker-runtime';
import external from '@smaltdb/external';
import parquet from '../../src/index.js';

serve({ plugins: [external, parquet] });
