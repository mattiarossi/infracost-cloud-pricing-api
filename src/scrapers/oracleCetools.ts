/**
 * oracle:cetools — Oracle's public cloud price list, the document the cost estimator and the
 * price list page render from: https://apexapps.oracle.com/pls/apex/cetools/api/v1/products/
 * (anonymous GET; {lastUpdated, items[]}; an item = partNumber, displayName, metricName,
 * serviceCategory, description?, currencyCodeLocalizations[{currencyCode, prices[{model, value,
 * rangeMin?, rangeMax?}]}]). One product per part number (vendor 'oracle', no region: a list
 * price is global, service 'OracleCloud', product family = the service category), one price per
 * USD price point (purchaseOption = the model, tier bounds from rangeMin/rangeMax, the part
 * number on the price). The document is cached as oracle-cetools-<lastUpdated>.json and loaded
 * once: a .loaded marker beside it, removed by --force.
 */
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import config from '../config';
import { Product, Price } from '../db/types';
import { generateProductHash, generatePriceHash } from '../db/helpers';
import { upsertProducts } from '../db/upsert';
import type { ScrapeResult } from './types';

export const CETOOLS_URL = 'https://apexapps.oracle.com/pls/apex/cetools/api/v1/products/';
const FILE_PREFIX = 'oracle-cetools-';
const VENDOR = 'oracle';
const SERVICE = 'OracleCloud';

type PricePoint = { model: string; value: number; rangeMin?: number; rangeMax?: number };
type Localization = { currencyCode: string; prices: PricePoint[] };
export type CetoolsItem = {
  partNumber: string;
  displayName: string;
  metricName: string;
  serviceCategory: string;
  description?: string;
  currencyCodeLocalizations: Localization[];
};
export type CetoolsDocument = { lastUpdated: string; items: CetoolsItem[] };

function loadedMarker(filename: string): string {
  return `${filename}.loaded`;
}

async function scrape(): Promise<ScrapeResult> {
  const force = process.argv.includes('--force') || process.argv.includes('-f');
  const filename = await download();
  if (force) fs.rmSync(loadedMarker(filename), { force: true });
  if (fs.existsSync(loadedMarker(filename))) {
    config.logger.info(`oracle:cetools: ${path.basename(filename)} already loaded`);
    return { failures: [] };
  }
  try {
    await load(filename);
    fs.writeFileSync(loadedMarker(filename), '');
    return { failures: [] };
  } catch (e) {
    const error = e instanceof Error ? e.message : String(e);
    config.logger.error(`oracle:cetools: ${path.basename(filename)}: ${error}`);
    return { failures: [{ file: filename, error }] };
  }
}

/** the document, written to the cache under its lastUpdated stamp when that stamp is new */
async function download(): Promise<string> {
  config.logger.info(`Downloading ${CETOOLS_URL}`);
  const resp = await axios.get<CetoolsDocument>(CETOOLS_URL, { responseType: 'json' });
  const doc = resp.data;
  if (!doc || typeof doc.lastUpdated !== 'string' || !Array.isArray(doc.items) || doc.items.length === 0) {
    throw new Error('oracle:cetools: the document has no lastUpdated or no items');
  }
  const stamp = doc.lastUpdated.replace(/[^0-9]/g, '');
  const filename = path.join(config.dataDir, `${FILE_PREFIX}${stamp}.json`);
  if (!fs.existsSync(filename)) {
    fs.writeFileSync(filename, JSON.stringify(doc));
    config.logger.info(`oracle:cetools: ${doc.items.length} items at ${doc.lastUpdated} -> ${path.basename(filename)}`);
  }
  return filename;
}

async function load(filename: string): Promise<void> {
  config.logger.info(`Processing file ${filename}`);
  const doc = JSON.parse(fs.readFileSync(filename, 'utf8')) as CetoolsDocument;
  const products = doc.items.map((item) => parseProduct(item, doc.lastUpdated));
  await upsertProducts(products);
}

export function parseProduct(item: CetoolsItem, lastUpdated: string): Product {
  const product: Product = {
    productHash: '',
    sku: item.partNumber,
    vendorName: VENDOR,
    region: null,
    service: SERVICE,
    productFamily: item.serviceCategory,
    attributes: {
      displayName: item.displayName,
      metricName: item.metricName,
      serviceCategory: item.serviceCategory,
      description: item.description ?? '',
      lastUpdated,
    },
    prices: [],
  };
  product.productHash = generateProductHash(product);
  product.prices = parsePrices(product, item, lastUpdated);
  return product;
}

function parsePrices(product: Product, item: CetoolsItem, lastUpdated: string): Price[] {
  const usd = item.currencyCodeLocalizations.find((c) => c.currencyCode === 'USD');
  if (!usd) return [];
  return usd.prices.map((point) => {
    const price: Price = {
      priceHash: '',
      purchaseOption: point.model,
      unit: item.metricName,
      USD: `${point.value}`,
      effectiveDateStart: lastUpdated,
      partNumber: item.partNumber,
    };
    if (point.rangeMin !== undefined) price.startUsageAmount = `${point.rangeMin}`;
    if (point.rangeMax !== undefined) price.endUsageAmount = `${point.rangeMax}`;
    price.priceHash = generatePriceHash(product, price);
    return price;
  });
}

export default {
  scrape,
};
