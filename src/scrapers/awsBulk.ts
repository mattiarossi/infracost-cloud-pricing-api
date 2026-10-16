import fs from 'fs';
import path from 'path';
import _ from 'lodash';
import axios from 'axios';
import glob from 'glob';
import format from 'pg-format';
import config from '../config';
import { Product, Price } from '../db/types';
import { generateProductHash, generatePriceHash } from '../db/helpers';
import { upsertProducts, upsertProductPricesOnly } from '../db/upsert';
import type { ScrapeResult } from './types';

const baseUrl = 'https://pricing.us-east-1.amazonaws.com';
const indexUrl = '/offers/v1.0/aws/index.json';
const chinaIndexUrl = '/offers/v1.0/cn/index.json';
const splitByRegions = ['AmazonEC2'];
/** the Region a testing run takes by default: every region-split and savings-plan offer publishes it */
const TESTING_REGION = 'us-east-1';

/** Extract the publication timestamp from a versioned AWS pricing URL, e.g. /offers/v1.0/aws/AmazonEC2/20260224205727/us-east-1/index.json → '20260224205727'.
 *  A URL without one is refused: a file named without its version is found by the existence check on every later run and never refreshed. */
function extractVersion(url: string): string {
  const match = url.match(/\/(\d{14})\//);
  if (!match) {
    throw new Error(`no publication version in ${url}: a file named without one would never be refreshed`);
  }
  return match[1];
}

/** Delete any previously downloaded files for the same service/region that have a different version timestamp,
 *  and the version-less `-current` file earlier releases wrote for every single-file service (left in place it
 *  would sort after the versioned file and, under --force, load after it). */
function cleanupOldVersions(currentFilename: string): void {
  // the pattern is the file name alone, matched inside its directory: a directory name is never read as a pattern
  const pattern = path.basename(currentFilename).replace(/\d{14}/, '{??????????????,current}');
  const siblings = glob.sync(pattern, { cwd: path.dirname(currentFilename), absolute: true });
  for (const sibling of siblings) {
    if (sibling !== currentFilename) {
      fs.unlinkSync(sibling);
      config.logger.info(`Deleted old version ${sibling}`);
      const marker = loadedMarker(sibling);
      if (fs.existsSync(marker)) {
        fs.unlinkSync(marker);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// .loaded sentinel helpers
// ---------------------------------------------------------------------------

function loadedMarker(filename: string): string {
  return `${filename}.loaded`;
}

function isAlreadyLoaded(filename: string): boolean {
  return fs.existsSync(loadedMarker(filename));
}

function markAsLoaded(filename: string): void {
  fs.writeFileSync(loadedMarker(filename), '');
}

/** Extract the offer code from a data filename.
 *  e.g. data/aws-AmazonEC2-us-east-1-20260224205727.json        → 'AmazonEC2'
 *       data/aws-AWSComputeSavingsPlan-savings-plan-us-east-1-... → 'AWSComputeSavingsPlan'
 */
function extractOfferCode(filename: string): string {
  const basename = filename.replace(/^.*[\/](aws|awscn)-/, '');
  return basename.split('-')[0];
}

/** Extract the savings-plan offer code from its index URL, e.g.
 *  /savingsPlan/v1.0/aws/AWSComputeSavingsPlan/current/region_index.json
 *    → 'AWSComputeSavingsPlan'
 *  AWS cross-references the same savings-plan index from many service offers;
 *  naming downloaded files after this code (rather than the service offer code)
 *  is what lets the de-dup collapse the duplicates onto one file per offer.
 *  Falls back to 'UnknownSavingsPlan' if the URL shape is unexpected. */
function extractSpOfferCode(indexUrl: string): string {
  const match = indexUrl.match(/savingsPlan\/v[\d.]+\/[^/]+\/([^/]+)\//);
  return match ? match[1] : 'UnknownSavingsPlan';
}

/** Return true for standard AWS region codes (e.g. "us-east-1", "ap-northeast-1")
 *  and false for Local Zones ("us-east-1-phl-1"), Wavelength Zones
 *  ("us-east-1-wl1-chi1"), and similar sub-region variants.
 *
 *  Main regions follow the pattern: {area}-{direction}-{number}
 *  e.g. us-east-1, eu-west-2, ap-northeast-1, me-south-1, af-south-1, il-central-1
 */
function isMainRegion(regionCode: string): boolean {
  return /^[a-z]{2}-(north|south|east|west|central|northeast|northwest|southeast|southwest)(east|west)?-\d+$/.test(regionCode);
}

function matchesService(filename: string, services: string[]): boolean {
  const offerCode = extractOfferCode(filename);
  return services.some((s) => s.toLowerCase() === offerCode.toLowerCase());
}

/** Match a savings-plan file against a --services request. Savings-plan files
 *  are named after the savings-plan offer (e.g. AWSComputeSavingsPlan), so a
 *  request for a service offer code (e.g. AmazonEC2) is translated through the
 *  service → savings-plan-offer map built during download. A request naming the
 *  savings-plan offer directly (e.g. AWSDatabaseSavingsPlans) also matches.
 *  Comparison is case-insensitive on both the file's offer code and the map. */
/** the Region of a region-split or savings-plan file (aws-AmazonEC2-<region>-<v>.json, aws-<offer>-savings-plan-<region>-<v>.json); null for a single-file offer */
function fileRegion(filename: string): string | null {
  const base = path.basename(filename);
  const sp = /-savings-plan-(.+)-\d{14}\.json$/.exec(base);
  if (sp) return sp[1];
  const split = new RegExp(`^(?:aws|awscn)-(?:${splitByRegions.join('|')})-(.+)-\\d{14}\\.json$`).exec(base);
  return split ? split[1] : null;
}

/** what a testing run loads: the AWS partition's single-file offers, and the testing Region's files of the others */
function inTestingScope(filename: string, options: LoadOptions): boolean {
  if (!path.basename(filename).startsWith('aws-')) return false;
  const region = fileRegion(filename);
  return region === null || region === options.testingRegion;
}

function matchesSavingsPlanService(
  filename: string,
  services: string[],
  serviceToSpOffer: Map<string, string>
): boolean {
  const spOfferCode = extractOfferCode(filename).toLowerCase();
  return services.some((s) => {
    const requested = s.toLowerCase();
    if (requested === spOfferCode) return true;
    for (const [service, spOffer] of serviceToSpOffer) {
      if (service.toLowerCase() === requested) {
        return spOffer.toLowerCase() === spOfferCode;
      }
    }
    return false;
  });
}

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

interface LoadOptions {
  /** Force reload of already-loaded files (ignore .loaded markers) */
  force: boolean;
  /** Process only standard pricing files (skip savings plan) */
  onlyStandard: boolean;
  /** Process only savings plan files (skip standard) */
  onlySavingsPlan: boolean;
  /** If non-empty, only load files whose offer code appears in this list */
  services: string[];
  /** One file per offer: of a region-split or savings-plan offer only testingRegion's file is downloaded and loaded; the China partition is not read */
  testing: boolean;
  /** the Region a testing run takes: TESTING_REGION unless --testing-region names another */
  testingRegion: string;
}

function parseArgs(): LoadOptions {
  const args = process.argv.slice(2);
  const options: LoadOptions = {
    force: false,
    onlyStandard: false,
    onlySavingsPlan: false,
    services: [],
    testing: false,
    testingRegion: TESTING_REGION,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--force' || arg === '-f') {
      options.force = true;
    } else if (arg === '--testing') {
      options.testing = true;
    } else if (arg === '--testing-region' && args[i + 1] && !args[i + 1].startsWith('-')) {
      options.testingRegion = args[i + 1];
      i += 1;
    } else if (arg === '--only-standard') {
      options.onlyStandard = true;
    } else if (arg === '--only-savings-plan') {
      options.onlySavingsPlan = true;
    } else if ((arg === '--services' || arg === '-s') && args[i + 1] && !args[i + 1].startsWith('-')) {
      options.services = args[i + 1].split(',').map((s) => s.trim()).filter(Boolean);
      i += 1;
    }
  }

  return options;
}

/** the region of a product priced across regions (no attributes.regionCode: data transfer, location "Any") */
const GLOBAL_REGION = 'aws-global';


type ProductJson = {
  sku: string;
  productFamily: string;
  attributes: {
    location: string;
    servicecode: string;
    /** the Region or zone code: us-east-1, us-east-1-bos-1, eu-west-2-wl1-lon1; absent on a product priced across regions (data transfer, location "Any") */
    regionCode?: string;
  } & { [key: string]: string };
};

type PriceJson = {
  effectiveDate: string;
  priceDimensions: {
    [key: string]: {
      unit: string;
      beginRange: string;
      endRange: string;
      description: string;
      pricePerUnit: {
        USD?: string;
        CNY?: string;
      };
    };
  };
  termAttributes?: {
    LeaseContractLength?: string;
    PurchaseOption?: string;
    OfferingClass?: string;
  };
};

type ServiceJson = {
  products: { [key: string]: ProductJson };
  terms: {
    OnDemand: { [key: string]: { [key: string]: PriceJson } };
    Reserved: { [key: string]: { [key: string]: PriceJson } };
  };
};

type SavingsPlanProductJson = {
  sku: string;
  productFamily: string;
  serviceCode: string;
  attributes: { [key: string]: string };
};

type SavingsPlanRateJson = {
  discountedSku: string;
  discountedUsageType: string;
  discountedOperation: string;
  discountedServiceCode: string;
  rateCode: string;
  unit: string;
  discountedRate: {
    currency: string;
    price: string;
  };
  discountedRegionCode: string;
  discountedInstanceType?: string;
};

type SavingsPlanTermJson = {
  sku: string;
  description: string;
  effectiveDate: string;
  leaseContractLength: string;
  rates: SavingsPlanRateJson[];
};

type SavingsPlanServiceJson = {
  regionCode?: string;
  products: SavingsPlanProductJson[];
  terms: {
    savingsPlan: SavingsPlanTermJson[];
  };
};

// ---------------------------------------------------------------------------
// Run stats
// ---------------------------------------------------------------------------

/** the run's counts and the files that failed to load; the statistics are the scrape's result (./types) */
export interface RunStats extends ScrapeResult {
  download: {
    standardDownloaded: number;
    standardCached: number;
    savingsPlanDownloaded: number;
    savingsPlanCached: number;
    savingsPlanZonesSkipped: number;
    savingsPlanIndexesSkipped: number;
  };
  load: {
    standard: { processed: number; cached: number; filtered: number; errors: number; productsUpserted: number; productsSkippedNoService: number; productsGlobal: number };
    savingsPlan: { processed: number; cached: number; filtered: number; errors: number; skippedNoProducts: number; productsUpdated: number; ratesMatched: number; ratesSkipped: number };
  };
  /** the Region of a testing run (--testing); null for a run over the whole price list */
  testingRegion: string | null;
}

function createStats(): RunStats {
  return {
    download: { standardDownloaded: 0, standardCached: 0, savingsPlanDownloaded: 0, savingsPlanCached: 0, savingsPlanZonesSkipped: 0, savingsPlanIndexesSkipped: 0 },
    load: {
      standard: { processed: 0, cached: 0, filtered: 0, errors: 0, productsUpserted: 0, productsSkippedNoService: 0, productsGlobal: 0 },
      savingsPlan: { processed: 0, cached: 0, filtered: 0, errors: 0, skippedNoProducts: 0, productsUpdated: 0, ratesMatched: 0, ratesSkipped: 0 },
    },
    failures: [],
    testingRegion: null,
  };
}

function printSummary(stats: RunStats): void {
  const dl = stats.download;
  const std = stats.load.standard;
  const sp = stats.load.savingsPlan;

  config.logger.info('');
  config.logger.info('======================= RUN SUMMARY =======================');
  if (stats.testingRegion) config.logger.info(`  Testing             : one file per offer, region ${stats.testingRegion}`);
  config.logger.info('DOWNLOAD');
  config.logger.info(`  Standard pricing    : ${dl.standardDownloaded} downloaded, ${dl.standardCached} cached`);
  config.logger.info(`  Savings plan        : ${dl.savingsPlanDownloaded} downloaded, ${dl.savingsPlanCached} cached, ${dl.savingsPlanZonesSkipped} zones skipped, ${dl.savingsPlanIndexesSkipped} duplicate indexes skipped`);
  config.logger.info('LOAD — Standard pricing');
  config.logger.info(`  Files               : ${std.processed} processed, ${std.cached} cached, ${std.filtered} filtered, ${std.errors} errors`);
  config.logger.info(`  Products upserted   : ${std.productsUpserted.toLocaleString()}`);
  config.logger.info(`  Products skipped    : ${std.productsSkippedNoService.toLocaleString()} (no servicecode)`);
  config.logger.info(`  Products global     : ${std.productsGlobal.toLocaleString()} (no regionCode, stored as ${GLOBAL_REGION})`);
  config.logger.info('LOAD — Savings plans');
  config.logger.info(`  Files               : ${sp.processed} processed, ${sp.cached} cached, ${sp.filtered} filtered, ${sp.errors} errors, ${sp.skippedNoProducts} skipped (no standard products)`);
  config.logger.info(`  Products updated    : ${sp.productsUpdated.toLocaleString()}`);
  config.logger.info(`  Rates               : ${sp.ratesMatched.toLocaleString()} matched, ${sp.ratesSkipped.toLocaleString()} skipped`);
  config.logger.info('===========================================================');
}

/** Normalize a savings-plan leaseContractLength (which may be an object like
 *  {"duration":1,"unit":"year"}) into the string format used by reserved
 *  pricing ("1yr", "3yr"). Falls back to the value as-is if already a string. */
function normalizeTermLength(raw: any): string | undefined {
  if (!raw) return undefined;
  if (typeof raw === 'string') return raw;
  if (typeof raw === 'object' && raw.duration && raw.unit) {
    const unit = String(raw.unit).toLowerCase();
    const abbrev = unit.startsWith('year') ? 'yr' : unit.startsWith('month') ? 'mo' : unit;
    return `${raw.duration}${abbrev}`;
  }
  return String(raw);
}

/** download every AWS offer and load the files not yet marked loaded; the run's statistics, with the files that
 *  failed to load, are the result (dataScrape judges the run by them and writes them into its summary) */
async function scrape(): Promise<RunStats> {
  const options = parseArgs();
  const stats = createStats();
  config.logger.info(
    `Load options — force: ${options.force}, onlyStandard: ${options.onlyStandard}, onlySavingsPlan: ${options.onlySavingsPlan}, services: ${options.services.length ? options.services.join(',') : '(all)'}, testing: ${options.testing ? options.testingRegion : 'no'}`
  );
  stats.testingRegion = options.testing ? options.testingRegion : null;
  const serviceToSpOffer = await downloadAll(stats, options);
  await loadAll(options, stats, serviceToSpOffer);
  printSummary(stats);
  return stats;
}

async function downloadAll(stats: RunStats, options: LoadOptions): Promise<Map<string, string>> {
  // Maps each service offer code to the savings-plan offer that covers it
  // (derived from the shared currentSavingsPlanIndexUrl). Consumed by the
  // --services translation in loadAll, because savings-plan files are now named
  // after the savings-plan offer, not the service offer.
  const serviceToSpOffer = new Map<string, string>();
  // De-dup set: AWS references one savings-plan index from many service offers
  // (e.g. AWSComputeSavingsPlan from EC2/ECS/EKS/Lambda). Handle each distinct
  // index once per run. Keyed by prefix+URL so the aws and awscn partitions
  // never collide.
  const handledSpIndexes = new Set<string>();

  // a testing run takes one Region of every region-split and savings-plan offer
  const onlyRegion = options.testing ? options.testingRegion : null;

  // Download standard AWS regions
  let indexResp = await axios.get(`${baseUrl}${indexUrl}`);
  for (const offer of <Offer[]>Object.values(indexResp.data.offers)) {
    await downloadService(offer, stats, 'aws', onlyRegion);
    await downloadSavingsPlan(offer, stats, 'aws', serviceToSpOffer, handledSpIndexes, onlyRegion);
  }

  // Download AWS China regions — not in a testing run: the partition publishes none of the AWS Regions
  if (!options.testing) {
    indexResp = await axios.get(`${baseUrl}${chinaIndexUrl}`);
    for (const offer of <Offer[]>Object.values(indexResp.data.offers)) {
      await downloadService(offer, stats, 'awscn');
      await downloadSavingsPlan(offer, stats, 'awscn', serviceToSpOffer, handledSpIndexes);
    }
  }

  return serviceToSpOffer;
}

interface Offer {
  offerCode: string;
  versionIndexUrl: string;
  currentRegionIndexUrl: string;
  currentVersionUrl: string;
  currentSavingsPlanIndexUrl: string;
}

/** An offer's version index (`versionIndexUrl`): every publication of the offer with its own URL, and which one is current. */
interface VersionIndex {
  currentVersion: string;
  versions: { [version: string]: { offerVersionUrl: string } };
}

interface Region {
  regionCode: string;
  currentVersionUrl: string;
}

interface SavingsPlanRegion {
  regionCode: string;
  versionUrl: string;
}

async function downloadService(offer: Offer, stats: RunStats, prefix?: string, onlyRegion: string | null = null) {
  if (!prefix) {
    prefix = 'aws'; // eslint-disable-line no-param-reassign
  }

  if (_.includes(splitByRegions, offer.offerCode)) {
    const regionResp = await axios.get(
      `${baseUrl}${offer.currentRegionIndexUrl}`
    );
    for (const region of <Region[]>Object.values(regionResp.data.regions)) {
      if (onlyRegion && region.regionCode !== onlyRegion) continue;
      const version = extractVersion(region.currentVersionUrl);
      const filename = path.join(config.dataDir, `${prefix}-${offer.offerCode}-${region.regionCode}-${version}.json`);
      if (fs.existsSync(filename)) {
        config.logger.info(`Skipping already downloaded ${filename}`);
        stats.download.standardCached++;
        continue;
      }
      config.logger.info(`Downloading ${region.currentVersionUrl}`);
      const resp = await axios({
        method: 'get',
        url: `${baseUrl}${region.currentVersionUrl}`,
        responseType: 'stream',
      });
      const writer = fs.createWriteStream(filename);
      resp.data.pipe(writer);
      await new Promise((resolve) => {
        writer.on('finish', resolve);
      });
      cleanupOldVersions(filename);
      stats.download.standardDownloaded++;
    }
  } else {
    // currentVersionUrl is `/<offer>/current/index.json` for every offer of both partitions and
    // carries no publication version; the offer's version index names the current one, and the
    // file is named with it and fetched from that publication's own URL
    const versionIndex = <VersionIndex>(await axios.get(`${baseUrl}${offer.versionIndexUrl}`)).data;
    const current = versionIndex.versions[versionIndex.currentVersion];
    if (!current) {
      throw new Error(`${offer.offerCode}: version index names ${versionIndex.currentVersion} as current but lists no such version`);
    }
    const version = extractVersion(current.offerVersionUrl);
    const filename = path.join(config.dataDir, `${prefix}-${offer.offerCode}-${version}.json`);
    if (fs.existsSync(filename)) {
      config.logger.info(`Skipping already downloaded ${filename}`);
      stats.download.standardCached++;
      return;
    }
    config.logger.info(`Downloading ${current.offerVersionUrl}`);
    const resp = await axios({
      method: 'get',
      url: `${baseUrl}${current.offerVersionUrl}`,
      responseType: 'stream',
    });
    const writer = fs.createWriteStream(filename);
    resp.data.pipe(writer);
    await new Promise((resolve) => {
      writer.on('finish', resolve);
    });
    cleanupOldVersions(filename);
    stats.download.standardDownloaded++;
  }
}

async function downloadSavingsPlan(
  offer: Offer,
  stats: RunStats,
  prefix = 'aws',
  serviceToSpOffer?: Map<string, string>,
  handledSpIndexes?: Set<string>,
  onlyRegion: string | null = null
) {
  if (!offer.currentSavingsPlanIndexUrl) {
    return;
  }

  const spOfferCode = extractSpOfferCode(offer.currentSavingsPlanIndexUrl);

  // Record the service → savings-plan-offer mapping for --services translation,
  // even when this index is a duplicate we skip downloading below.
  if (serviceToSpOffer) {
    serviceToSpOffer.set(offer.offerCode, spOfferCode);
  }

  // De-dup: the same savings-plan index is referenced by many service offers.
  // Download it once per run; subsequent referencing offers are skipped here.
  const dedupKey = `${prefix}|${offer.currentSavingsPlanIndexUrl}`;
  if (handledSpIndexes) {
    if (handledSpIndexes.has(dedupKey)) {
      config.logger.info(
        `Skipping duplicate savings-plan index ${offer.currentSavingsPlanIndexUrl} (already handled this run; also referenced by ${offer.offerCode})`
      );
      stats.download.savingsPlanIndexesSkipped++;
      return;
    }
    handledSpIndexes.add(dedupKey);
  }

  const indexResp = await axios.get(`${baseUrl}${offer.currentSavingsPlanIndexUrl}`);
  const regions: SavingsPlanRegion[] = indexResp.data && indexResp.data.regions;

  if (!Array.isArray(regions) || regions.length === 0) {
    config.logger.warn(
      `No regions found in savings plan index for ${offer.offerCode} at ${offer.currentSavingsPlanIndexUrl}`
    );
    return;
  }

  // Filter out Local Zones, Wavelength Zones, and other sub-region variants.
  // These have extra segments in their regionCode (e.g. "us-east-1-phl-1",
  // "us-east-1-wl1-chi1") while main regions have the standard 3-segment
  // form (e.g. "us-east-1", "eu-west-2", "ap-northeast-1").
  //
  // The zones' standard price lists load under their zone codes; their savings
  // plans are not loaded: the rates name zone-prefixed usage types
  // (e.g. "PHL1-BoxUsage:c5d.xlarge"), and their match to the zone products by
  // (usagetype, operation) is unverified. Loading them is relaxing this filter.
  const mainRegions = regions.filter((r) => isMainRegion(r.regionCode));
  const skippedCount = regions.length - mainRegions.length;
  stats.download.savingsPlanZonesSkipped += skippedCount;
  if (skippedCount > 0) {
    config.logger.info(
      `${offer.offerCode}: skipping ${skippedCount} local/wavelength zone savings plan files, keeping ${mainRegions.length} main regions`
    );
  }

  const wanted = onlyRegion ? mainRegions.filter((r) => r.regionCode === onlyRegion) : mainRegions;
  for (const region of wanted) {
    const version = extractVersion(region.versionUrl);
    const filename = path.join(config.dataDir, `${prefix}-${spOfferCode}-savings-plan-${region.regionCode}-${version}.json`);
    if (fs.existsSync(filename)) {
      config.logger.info(`Skipping already downloaded ${filename}`);
      stats.download.savingsPlanCached++;
      continue;
    }
    config.logger.info(`Downloading savings plan ${region.versionUrl}`);
    const resp = await axios({
      method: 'get',
      url: `${baseUrl}${region.versionUrl}`,
      responseType: 'stream',
    });
    const writer = fs.createWriteStream(filename);
    resp.data.pipe(writer);
    await new Promise((resolve) => {
      writer.on('finish', resolve);
    });
    cleanupOldVersions(filename);
    stats.download.savingsPlanDownloaded++;
  }
}

async function loadAll(
  options: LoadOptions,
  stats: RunStats,
  serviceToSpOffer: Map<string, string> = new Map()
): Promise<void> {
  const allFiles = glob.sync('aws*.json', { cwd: config.dataDir, absolute: true }).filter((f) => !f.endsWith('.loaded'));
  const savingsPlanFiles = allFiles.filter((filename) =>
    filename.includes('-savings-plan-') || filename.endsWith('-savings-plan.json')
  );
  const standardFiles = allFiles.filter(
    (filename) => !savingsPlanFiles.includes(filename)
  );

  const processStandard = !options.onlySavingsPlan;
  const processSavingsPlan = !options.onlyStandard;

  if (processStandard) {
    for (const filename of standardFiles) {
      if (options.testing && !inTestingScope(filename, options)) {
        stats.load.standard.filtered++;
        continue;
      }
      if (options.services.length > 0 && !matchesService(filename, options.services)) {
        stats.load.standard.filtered++;
        continue;
      }
      if (!options.force && isAlreadyLoaded(filename)) {
        config.logger.info(`Skipping already loaded ${filename}`);
        stats.load.standard.cached++;
        continue;
      }
      config.logger.info(`Processing file: ${filename}`);
      try {
        await processFile(filename, stats);
        markAsLoaded(filename);
        stats.load.standard.processed++;
      } catch (e: any) {
        config.logger.error(`Skipping file ${filename} due to error ${e}`);
        config.logger.error(e.stack);
        stats.load.standard.errors++;
        stats.failures.push({ file: filename, error: String(e) });
      }
    }
  }

  if (processSavingsPlan) {
    for (const filename of savingsPlanFiles) {
      if (options.testing && !inTestingScope(filename, options)) {
        stats.load.savingsPlan.filtered++;
        continue;
      }
      if (options.services.length > 0 && !matchesSavingsPlanService(filename, options.services, serviceToSpOffer)) {
        stats.load.savingsPlan.filtered++;
        continue;
      }
      if (!options.force && isAlreadyLoaded(filename)) {
        config.logger.info(`Skipping already loaded ${filename}`);
        stats.load.savingsPlan.cached++;
        continue;
      }
      config.logger.info(`Processing file: ${filename}`);
      try {
        await processFile(filename, stats);
        markAsLoaded(filename);
      } catch (e: any) {
        config.logger.error(`Skipping file ${filename} due to error ${e}`);
        config.logger.error(e.stack);
        stats.load.savingsPlan.errors++;
        stats.failures.push({ file: filename, error: String(e) });
      }
    }
  }
}

async function processFile(filename: string, stats: RunStats): Promise<void> {
  const body = fs.readFileSync(filename);
  const json = JSON.parse(body.toString());

  if (isSavingsPlanServiceJson(json)) {
    await processSavingsPlanFile(json, filename, stats);
    return;
  }

  const serviceJson = <ServiceJson>json;

  // a product without attributes.servicecode has no value for the NOT NULL service column: skipped
  // with one warning per file instead of failing the file (some offers carry it on no product)
  const productJsons = Object.values(serviceJson.products);
  const withService = productJsons.filter((p) => p.attributes && p.attributes.servicecode);
  const skipped = productJsons.length - withService.length;
  if (skipped > 0) {
    config.logger.warn(`${filename}: ${skipped} of ${productJsons.length} products carry no attributes.servicecode and are skipped`);
    stats.load.standard.productsSkippedNoService += skipped;
  }
  // a product without attributes.regionCode is priced across regions (data transfer, location "Any"):
  // stored under GLOBAL_REGION (parseProduct) and counted for the run summary
  stats.load.standard.productsGlobal += withService.filter((p) => !p.attributes.regionCode).length;
  const products = withService.map((productJson) => {
    const product = parseProduct(productJson);

    if (serviceJson.terms.OnDemand && serviceJson.terms.OnDemand[product.sku]) {
      product.prices.push(
        ...parsePrices(
          product,
          serviceJson.terms.OnDemand[product.sku],
          'on_demand'
        )
      );
    }

    if (serviceJson.terms.Reserved && serviceJson.terms.Reserved[product.sku]) {
      product.prices.push(
        ...parsePrices(
          product,
          serviceJson.terms.Reserved[product.sku],
          'reserved'
        )
      );
    }

    return product;
  });

  const rows = await upsertProducts(products);
  stats.load.standard.productsUpserted += rows;
}

function isSavingsPlanServiceJson(json: any): json is SavingsPlanServiceJson {
  return Array.isArray(json?.terms?.savingsPlan);
}

/**
 * Build a lookup of (usagetype, operation) → productHash from the DB for a
 * given service+region.  Used for EC2 savings plans where discountedSku does
 * NOT match the standard product SKU.
 */
async function buildAttributeLookup(
  service: string,
  region: string
): Promise<Map<string, string>> {
  const pool = await config.pg();
  const result = await pool.query(
    format(
      `SELECT "productHash",
              "attributes"->>'usagetype'  AS ut,
              "attributes"->>'operation'  AS op
       FROM %I
       WHERE "service" = %L AND "region" = %L`,
      config.productTableName,
      service,
      region
    )
  );
  const lookup = new Map<string, string>();
  for (const row of result.rows) {
    if (row.ut != null && row.op != null) {
      lookup.set(`${row.ut}\t${row.op}`, row.productHash);
    }
  }
  return lookup;
}

async function processSavingsPlanFile(
  json: SavingsPlanServiceJson,
  filename: string,
  stats: RunStats
): Promise<void> {
  const savingsPlanProductsBySku = new Map<string, SavingsPlanProductJson>();
  json.products.forEach((product) => {
    savingsPlanProductsBySku.set(product.sku, product);
  });

  // Determine the unique set of (service, region) pairs across all rates so we
  // can decide whether we need attribute-based matching for any of them.
  const serviceRegionPairs = new Set<string>();
  for (const term of json.terms.savingsPlan) {
    for (const rate of term.rates) {
      if (rate.discountedServiceCode && rate.discountedRegionCode) {
        serviceRegionPairs.add(`${rate.discountedServiceCode}\t${rate.discountedRegionCode}`);
      }
    }
  }

  // Early guard: verify that standard products exist in the DB for at least
  // one (service, region) pair.  If the table is empty for all of them the
  // savings-plan file was processed before its corresponding standard pricing
  // file — nothing useful can be done so bail out with a clear warning.
  const pool = await config.pg();
  const emptyPairs: string[] = [];
  for (const pair of serviceRegionPairs) {
    const [service, region] = pair.split('\t');
    const countResult = await pool.query(
      format(
        `SELECT 1 FROM %I WHERE "service" = %L AND "region" = %L LIMIT 1`,
        config.productTableName, service, region
      )
    );
    if (countResult.rowCount === 0) {
      emptyPairs.push(`${service}/${region}`);
    }
  }
  if (emptyPairs.length === serviceRegionPairs.size) {
    config.logger.warn(
      `Skipping ${filename}: no standard products loaded yet for any target service/region (${emptyPairs.join(', ')}). ` +
      `Load the standard pricing file first, then re-run with --only-savings-plan.`
    );
    stats.load.savingsPlan.skippedNoProducts++;
    return;
  }
  if (emptyPairs.length > 0) {
    config.logger.warn(
      `${filename}: no standard products for ${emptyPairs.join(', ')} — savings-plan rates targeting these will be skipped`
    );
  }

  // For each (service, region) pair, check whether discountedSku-based
  // matching works by probing the DB.  If not, load an attribute lookup.
  const attrLookups = new Map<string, Map<string, string>>();

  for (const pair of serviceRegionPairs) {
    const [service, region] = pair.split('\t');

    // Skip pairs we already know have no standard products
    if (emptyPairs.includes(`${service}/${region}`)) continue;

    // Probe: pick a discountedSku for this pair and see if its productHash exists
    let sampleSku: string | undefined;
    for (const term of json.terms.savingsPlan) {
      for (const rate of term.rates) {
        if (rate.discountedServiceCode === service && rate.discountedRegionCode === region && rate.discountedSku) {
          sampleSku = rate.discountedSku;
          break;
        }
      }
      if (sampleSku) break;
    }

    if (sampleSku) {
      const probeHash = generateProductHash({
        productHash: '', vendorName: 'aws', service, productFamily: '',
        region, sku: sampleSku, attributes: {}, prices: [],
      });
      const probe = await pool.query(
        format(`SELECT 1 FROM %I WHERE "productHash" = %L LIMIT 1`, config.productTableName, probeHash)
      );
      if (probe.rowCount === 0) {
        config.logger.info(`discountedSku does not match standard SKUs for ${service}/${region} — using attribute-based matching`);
        const lookup = await buildAttributeLookup(service, region);
        attrLookups.set(pair, lookup);
        config.logger.info(`  loaded ${lookup.size} (usagetype,operation) → productHash entries`);
      }
    }
  }

  // Accumulate full Product stubs keyed by productHash so that each stub
  // collects all of its savings-plan prices before we hit the DB once.
  const productsByHash: Map<string, Product> = new Map();
  let skipped = 0;

  json.terms.savingsPlan.forEach((term) => {
    const savingsPlanProduct = savingsPlanProductsBySku.get(term.sku);
    const termLength = normalizeTermLength(
      term.leaseContractLength || savingsPlanProduct?.attributes?.purchaseTerm
    );
    const termPurchaseOption =
      savingsPlanProduct?.attributes?.purchaseOption;

    term.rates.forEach((rate) => {
      if (!rate.discountedSku) {
        return;
      }

      const pairKey = `${rate.discountedServiceCode}\t${rate.discountedRegionCode}`;
      const attrLookup = attrLookups.get(pairKey);

      let productHash: string;
      let sku: string;

      if (attrLookup) {
        // Attribute-based matching (EC2 and similar)
        const lookupKey = `${rate.discountedUsageType}\t${rate.discountedOperation}`;
        const found = attrLookup.get(lookupKey);
        if (!found) {
          skipped++;
          return;
        }
        productHash = found;
        // We don't know the original SKU, but we need one for the stub.
        // Use discountedSku — it won't overwrite the real row because
        // upsertProductPricesOnly is UPDATE-only.
        sku = rate.discountedSku;
      } else {
        // Direct SKU-based matching (RDS and similar)
        const stub: Product = {
          productHash: '', vendorName: 'aws', service: rate.discountedServiceCode || '',
          productFamily: '', region: rate.discountedRegionCode || null,
          sku: rate.discountedSku, attributes: {}, prices: [],
        };
        productHash = generateProductHash(stub);
        sku = rate.discountedSku;
      }

      // Build a minimal product stub keyed by the resolved productHash
      const stub: Product = {
        productHash,
        vendorName: 'aws',
        service: rate.discountedServiceCode || '',
        productFamily: '',
        region: rate.discountedRegionCode || null,
        sku,
        attributes: {},
        prices: [],
      };

      const price: Price = {
        priceHash: '',
        purchaseOption: 'savings_plan',
        unit: rate.unit,
        USD: rate.discountedRate && rate.discountedRate.price,
        effectiveDateStart: term.effectiveDate,
        description: term.description,
        termLength,
        termPurchaseOption,
        currency: rate.discountedRate && rate.discountedRate.currency,
        savingsPlanSku: term.sku,
        discountedSku: rate.discountedSku,
        discountedUsageType: rate.discountedUsageType,
        discountedOperation: rate.discountedOperation,
        discountedServiceCode: rate.discountedServiceCode,
        discountedRegionCode: rate.discountedRegionCode,
        discountedInstanceType: rate.discountedInstanceType,
        rateCode: rate.rateCode,
      };
      price.priceHash = generatePriceHash(stub, price);

      const existing = productsByHash.get(productHash);
      if (existing) {
        existing.prices.push(price);
      } else {
        stub.prices.push(price);
        productsByHash.set(productHash, stub);
      }
    });
  });

  stats.load.savingsPlan.ratesMatched += Array.from(productsByHash.values()).reduce((sum, p) => sum + p.prices.length, 0);
  stats.load.savingsPlan.ratesSkipped += skipped;

  if (skipped > 0) {
    config.logger.warn(`${skipped} savings-plan rates could not be matched to standard products (${filename})`);
  }

  const rows = await upsertProductPricesOnly(Array.from(productsByHash.values()));
  stats.load.savingsPlan.productsUpdated += rows;
  stats.load.savingsPlan.processed++;
}

function parseProduct(productJson: ProductJson) {
  const product: Product = {
    productHash: '',
    vendorName: 'aws',
    service: productJson.attributes.servicecode,
    productFamily: productJson.productFamily,
    region: productJson.attributes.regionCode || GLOBAL_REGION,
    sku: productJson.sku,
    attributes: productJson.attributes,
    prices: [],
  };

  product.productHash = generateProductHash(product);

  return product;
}

function parsePrices(
  product: Product,
  priceData: { [key: string]: PriceJson },
  purchaseOption: string
): Price[] {
  const prices: Price[] = [];

  Object.values(priceData).forEach((priceItem) => {
    Object.values(priceItem.priceDimensions).forEach((priceDimension) => {
      const price: Price = {
        priceHash: '',
        purchaseOption,
        unit: priceDimension.unit,
        USD: priceDimension.pricePerUnit.USD,
        CNY: priceDimension.pricePerUnit.CNY,
        effectiveDateStart: priceItem.effectiveDate,
        startUsageAmount: priceDimension.beginRange,
        endUsageAmount: priceDimension.endRange,
        description: priceDimension.description,
      };

      if (purchaseOption === 'reserved') {
        Object.assign(price, {
          termLength:
            priceItem.termAttributes &&
            priceItem.termAttributes.LeaseContractLength,
          termPurchaseOption:
            priceItem.termAttributes && priceItem.termAttributes.PurchaseOption,
          termOfferingClass:
            priceItem.termAttributes && priceItem.termAttributes.OfferingClass,
        });
      }

      price.priceHash = generatePriceHash(product, price);

      prices.push(price);
    });
  });

  return prices;
}

export default {
  scrape,
};
