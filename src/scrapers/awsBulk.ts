import fs from 'fs';
import _ from 'lodash';
import axios from 'axios';
import glob from 'glob';
import format from 'pg-format';
import config from '../config';
import { Product, Price } from '../db/types';
import { generateProductHash, generatePriceHash } from '../db/helpers';
import { upsertProducts, upsertProductPricesOnly } from '../db/upsert';

const baseUrl = 'https://pricing.us-east-1.amazonaws.com';
const indexUrl = '/offers/v1.0/aws/index.json';
const chinaIndexUrl = '/offers/v1.0/cn/index.json';
const splitByRegions = ['AmazonEC2'];

/** Extract the publication timestamp from a versioned AWS pricing URL, e.g. /offers/v1.0/aws/AmazonEC2/20260224205727/us-east-1/index.json → '20260224205727' */
function extractVersion(url: string): string {
  const match = url.match(/\/(\d{14})\//);
  return match ? match[1] : 'current';
}

/** Delete any previously downloaded files for the same service/region that have a different version timestamp. */
function cleanupOldVersions(currentFilename: string): void {
  const pattern = currentFilename.replace(/\d{14}/, '??????????????');
  const siblings = glob.sync(pattern);
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
}

function parseArgs(): LoadOptions {
  const args = process.argv.slice(2);
  const options: LoadOptions = {
    force: false,
    onlyStandard: false,
    onlySavingsPlan: false,
    services: [],
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--force' || arg === '-f') {
      options.force = true;
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

const regionMapping: { [key: string]: string } = {
  'AWS GovCloud (US)': 'us-gov-west-1',
  'AWS GovCloud (US-West)': 'us-gov-west-1',
  'AWS GovCloud (US-East)': 'us-gov-east-1',
  'US East (N. Virginia)': 'us-east-1',
  'US East (Ohio)': 'us-east-2',
  'US West (N. California)': 'us-west-1',
  'US West (Oregon)': 'us-west-2',
  'US West (Los Angeles)': 'us-west-2-lax-1',
  'US ISO East': 'us-iso-east-1',
  'US ISOB East (Ohio)': 'us-isob-east-1',
  'Canada (Central)': 'ca-central-1',
  'China (Beijing)': 'cn-north-1',
  'China (Ningxia)': 'cn-northwest-1',
  'EU (Frankfurt)': 'eu-central-1',
  'Europe (Zurich)': 'eu-central-2',
  'EU (Ireland)': 'eu-west-1',
  'EU (London)': 'eu-west-2',
  'EU (Milan)': 'eu-south-1',
  'Europe (Spain)': 'eu-south-2',
  'EU (Paris)': 'eu-west-3',
  'EU (Stockholm)': 'eu-north-1',
  'Asia Pacific (Hong Kong)': 'ap-east-1',
  'Asia Pacific (Tokyo)': 'ap-northeast-1',
  'Asia Pacific (Seoul)': 'ap-northeast-2',
  'Asia Pacific (Osaka-Local)': 'ap-northeast-3',
  'Asia Pacific (Osaka)': 'ap-northeast-3',
  'Asia Pacific (Singapore)': 'ap-southeast-1',
  'Asia Pacific (Sydney)': 'ap-southeast-2',
  'Asia Pacific (Jakarta)': 'ap-southeast-3',
  'Asia Pacific (Mumbai)': 'ap-south-1',
  'Asia Pacific (Hyderabad)': 'ap-south-2',
  'Middle East (Bahrain)': 'me-south-1',
  'Middle East (UAE)': 'me-central-1',
  'South America (Sao Paulo)': 'sa-east-1',
  'Africa (Cape Town)': 'af-south-1',
  'Asia Pacific (Taipei)': 'ap-east-2',
};

type ProductJson = {
  sku: string;
  productFamily: string;
  attributes: {
    location: string;
    servicecode: string;
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

interface RunStats {
  download: {
    standardDownloaded: number;
    standardCached: number;
    savingsPlanDownloaded: number;
    savingsPlanCached: number;
    savingsPlanZonesSkipped: number;
    savingsPlanIndexesSkipped: number;
  };
  load: {
    standard: { processed: number; cached: number; filtered: number; errors: number; productsUpserted: number };
    savingsPlan: { processed: number; cached: number; filtered: number; errors: number; skippedNoProducts: number; productsUpdated: number; ratesMatched: number; ratesSkipped: number };
  };
}

function createStats(): RunStats {
  return {
    download: { standardDownloaded: 0, standardCached: 0, savingsPlanDownloaded: 0, savingsPlanCached: 0, savingsPlanZonesSkipped: 0, savingsPlanIndexesSkipped: 0 },
    load: {
      standard: { processed: 0, cached: 0, filtered: 0, errors: 0, productsUpserted: 0 },
      savingsPlan: { processed: 0, cached: 0, filtered: 0, errors: 0, skippedNoProducts: 0, productsUpdated: 0, ratesMatched: 0, ratesSkipped: 0 },
    },
  };
}

function printSummary(stats: RunStats): void {
  const dl = stats.download;
  const std = stats.load.standard;
  const sp = stats.load.savingsPlan;

  config.logger.info('');
  config.logger.info('======================= RUN SUMMARY =======================');
  config.logger.info('DOWNLOAD');
  config.logger.info(`  Standard pricing    : ${dl.standardDownloaded} downloaded, ${dl.standardCached} cached`);
  config.logger.info(`  Savings plan        : ${dl.savingsPlanDownloaded} downloaded, ${dl.savingsPlanCached} cached, ${dl.savingsPlanZonesSkipped} zones skipped, ${dl.savingsPlanIndexesSkipped} duplicate indexes skipped`);
  config.logger.info('LOAD — Standard pricing');
  config.logger.info(`  Files               : ${std.processed} processed, ${std.cached} cached, ${std.filtered} filtered, ${std.errors} errors`);
  config.logger.info(`  Products upserted   : ${std.productsUpserted.toLocaleString()}`);
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

async function scrape(): Promise<void> {
  const options = parseArgs();
  const stats = createStats();
  config.logger.info(
    `Load options — force: ${options.force}, onlyStandard: ${options.onlyStandard}, onlySavingsPlan: ${options.onlySavingsPlan}, services: ${options.services.length ? options.services.join(',') : '(all)'}`
  );
  const serviceToSpOffer = await downloadAll(stats);
  await loadAll(options, stats, serviceToSpOffer);
  printSummary(stats);
}

async function downloadAll(stats: RunStats): Promise<Map<string, string>> {
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

  // Download standard AWS regions
  let indexResp = await axios.get(`${baseUrl}${indexUrl}`);
  for (const offer of <Offer[]>Object.values(indexResp.data.offers)) {
    await downloadService(offer, stats);
    await downloadSavingsPlan(offer, stats, 'aws', serviceToSpOffer, handledSpIndexes);
  }

  // Download AWS China regions
  indexResp = await axios.get(`${baseUrl}${chinaIndexUrl}`);
  for (const offer of <Offer[]>Object.values(indexResp.data.offers)) {
    await downloadService(offer, stats, 'awscn');
    await downloadSavingsPlan(offer, stats, 'awscn', serviceToSpOffer, handledSpIndexes);
  }

  return serviceToSpOffer;
}

interface Offer {
  offerCode: string;
  currentRegionIndexUrl: string;
  currentVersionUrl: string;
  currentSavingsPlanIndexUrl: string;
}

interface Region {
  regionCode: string;
  currentVersionUrl: string;
}

interface SavingsPlanRegion {
  regionCode: string;
  versionUrl: string;
}

async function downloadService(offer: Offer, stats: RunStats, prefix?: string) {
  if (!prefix) {
    prefix = 'aws'; // eslint-disable-line no-param-reassign
  }

  if (_.includes(splitByRegions, offer.offerCode)) {
    const regionResp = await axios.get(
      `${baseUrl}${offer.currentRegionIndexUrl}`
    );
    for (const region of <Region[]>Object.values(regionResp.data.regions)) {
      const version = extractVersion(region.currentVersionUrl);
      const filename = `data/${prefix}-${offer.offerCode}-${region.regionCode}-${version}.json`;
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
    const version = extractVersion(offer.currentVersionUrl);
    const filename = `data/${prefix}-${offer.offerCode}-${version}.json`;
    if (fs.existsSync(filename)) {
      config.logger.info(`Skipping already downloaded ${filename}`);
      stats.download.standardCached++;
      return;
    }
    config.logger.info(`Downloading ${offer.currentVersionUrl}`);
    const resp = await axios({
      method: 'get',
      url: `${baseUrl}${offer.currentVersionUrl}`,
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
  handledSpIndexes?: Set<string>
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
  // We skip these because:
  //   1. The standard pricing files for local/wavelength zones use location
  //      names (e.g. "US East (Philadelphia)") that are not in regionMapping,
  //      so their products end up with region=null in the DB.
  //   2. Even if regionMapping were extended, the savings plan rates for these
  //      zones use zone-prefixed usageTypes (e.g. "PHL1-BoxUsage:c5d.xlarge")
  //      that don't match the main-region product attributes.
  //
  // TODO: To support local/wavelength zone savings plans in the future:
  //   a. Add zone location names to regionMapping (or auto-derive from the
  //      standard pricing file's regionCode field).
  //   b. Remove or relax this filter.
  const mainRegions = regions.filter((r) => isMainRegion(r.regionCode));
  const skippedCount = regions.length - mainRegions.length;
  stats.download.savingsPlanZonesSkipped += skippedCount;
  if (skippedCount > 0) {
    config.logger.info(
      `${offer.offerCode}: skipping ${skippedCount} local/wavelength zone savings plan files, keeping ${mainRegions.length} main regions`
    );
  }

  for (const region of mainRegions) {
    const version = extractVersion(region.versionUrl);
    const filename = `data/${prefix}-${spOfferCode}-savings-plan-${region.regionCode}-${version}.json`;
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
  const allFiles = glob.sync('data/aws*.json').filter((f) => !f.endsWith('.loaded'));
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
      }
    }
  }

  if (processSavingsPlan) {
    for (const filename of savingsPlanFiles) {
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

  const products = Object.values(serviceJson.products).map((productJson) => {
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
    region: regionMapping[productJson.attributes.location] || null,
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
