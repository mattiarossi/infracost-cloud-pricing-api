/**
 * dataScrape — the scrapers --only selects (vendor:source, comma-separated; every scraper when absent), one after
 * another, into the products table, the run recorded in the stats table.
 *
 *   node dist/cmd/dataScrape.js [--only aws:bulk,azure:retail] [--summary <file>] [the selected scrapers' own flags]
 *
 * The stats row records the update as not successful when the run starts and as successful, with its time, only
 * when every selected scraper finished and none reported a file it could not load; then the configured views
 * refresh. --summary writes each scraper's outcome — its error, or its result with the files it could not load —
 * as JSON. Exit 0 when every scraper succeeded, 1 otherwise. A scraper's module is loaded only when it runs.
 */

import fs from 'fs';
import yargs from 'yargs';
import config from '../config';
import { setPriceUpdateFailed, setPriceUpdateSuccessful } from '../stats/stats';
import { refreshViews } from '../db/refreshview';
import type { ScrapeResult } from '../scrapers/types';

type Scrape = () => Promise<ScrapeResult | void>;

/** every scraper by vendor and source; its module is imported when it runs, so a run of one vendor loads no other vendor's client */
const Scrapers: { [vendor: string]: { [source: string]: () => Promise<Scrape> } } = {
  aws: {
    bulk: async () => (await import('../scrapers/awsBulk')).default.scrape,
    spot: async () => (await import('../scrapers/awsSpot')).default.scrape,
  },
  azure: {
    retail: async () => (await import('../scrapers/azureRetail')).default.scrape,
  },
  gcp: {
    catalog: async () => (await import('../scrapers/gcpCatalog')).default.scrape,
    machineTypes: async () => (await import('../scrapers/gcpMachineTypes')).default.scrape,
  },
  ibm: {
    kubernetes: async () => (await import('../scrapers/ibmKubernetes')).default.scrape,
    catalog: async () => (await import('../scrapers/ibmCatalog')).default.scrape,
  },
};

/** one scraper's outcome in the summary */
interface ScraperOutcome {
  source: string;               // vendor:source
  ok: boolean;
  error: string | null;         // what stopped it; null when it finished
  result: ScrapeResult | null;  // what it returned (awsBulk: its run statistics and failed files)
}

async function run(): Promise<boolean> {
  const startedAt = new Date().toISOString();
  const argv = await yargs
    .usage(
      'Usage: $0 --only=[aws:bulk,aws:spot,azure:retail,gcp:catalog,gcp:machineTypes,ibm:kubernetes,ibm:catalog] [--summary <file>]'
    )
    .options({
      only: { type: 'string' },
      summary: { type: 'string' },
    }).argv;

  const selected: string[] = [];
  for (const [vendor, sources] of Object.entries(Scrapers)) {
    for (const source of Object.keys(sources)) {
      const id = `${vendor}:${source}`;
      if (!argv.only || argv.only.split(',').includes(id)) selected.push(id);
    }
  }
  // a name that selects nothing used to run nothing and record success
  const unknown = argv.only ? argv.only.split(',').filter((id) => !selected.includes(id)) : [];
  if (unknown.length) throw new Error(`--only names no scraper: ${unknown.join(', ')}`);

  // not successful from the start: a run that dies before its end leaves the row saying so
  await setPriceUpdateFailed();
  const outcomes: ScraperOutcome[] = [];
  for (const id of selected) {
    const [vendor, source] = id.split(':');
    config.logger.info(`Running update function for ${id}`);
    try {
      const scrape = await Scrapers[vendor][source]();
      const result = (await scrape()) || null;
      outcomes.push({ source: id, ok: !result || result.failures.length === 0, error: null, result });
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      config.logger.error(error);
      outcomes.push({ source: id, ok: false, error, result: null });
    }
  }

  const ok = outcomes.every((o) => o.ok);
  if (ok) {
    await setPriceUpdateSuccessful();
    const pool = await config.pg();
    const client = await pool.connect();
    await refreshViews(client);
  } else {
    await setPriceUpdateFailed();
  }
  if (argv.summary) {
    const summary = { startedAt, finishedAt: new Date().toISOString(), ok, dataDir: config.dataDir, scrapers: outcomes };
    fs.writeFileSync(argv.summary, `${JSON.stringify(summary, null, 2)}\n`);
  }
  return ok;
}

config.logger.info('Starting: scraping data from cloud vendors');
run()
  .then((ok) => {
    config.logger.info(`Completed: scraping data from cloud vendors${ok ? '' : ' with failures'}`);
    process.exit(ok ? 0 : 1);
  })
  .catch((err) => {
    config.logger.error(err);
    process.exit(1);
  });
