/**
 * aws:odbRegions — where Oracle Database@AWS is, as rows of public.products: one product per
 * AWS region (vendor 'aws', service 'AmazonODB', sku odb:region:<code>, region = the code, no
 * price) whose attributes carry the region's zones and, per service column of Oracle's regional
 * availability matrix, the zones that service has in the region — the column header verbatim as
 * the attribute name (svc:<header>), so a column Oracle adds appears at the next run with no
 * change here. Three sources: the AWS regional services table (the region set), the AWS user
 * guide's supported-regions tables (name, code, zone ids) and Oracle's matrix (name, zones per
 * service). The sources must agree on the region set and every Oracle row must be a region of
 * the guide — matched by the prefix of its zone ids (cac1-az1 → cac1), by its name second, since the
 * two pages spell a region's name differently and never a zone id; otherwise the run fails. The
 * assembled rows are written to the cache as a record and upserted on every run (22 rows; no load
 * marker).
 */
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import config from '../config';
import { Product } from '../db/types';
import { generateProductHash } from '../db/helpers';
import { upsertProducts } from '../db/upsert';
import type { ScrapeResult } from './types';

export const REGIONAL_TABLE_URL = 'https://api.regional-table.region-services.aws.a2z.com/index.json';
export const AWS_GUIDE_URL = 'https://docs.aws.amazon.com/odb/latest/UserGuide/getting-started.html';
export const ORACLE_MATRIX_URL = 'https://docs.oracle.com/en-us/iaas/Content/database-at-aws/oaaws-regions.htm';
/** the service the regional table names; the one string of the AWS side this module knows */
const REGIONAL_TABLE_SERVICE = 'Oracle Database@AWS';
const USER_AGENT = 'Mozilla/5.0 (compatible; cloud-pricing-api)';
const SERVICE_ATTRIBUTE_PREFIX = 'svc:';
const ZONE_ID = /[a-z]+\d+-az\d+/g;

type RegionalTable = { prices: { id: string; attributes: { [k: string]: string } }[] };
type Table = string[][];                       // rows of cell texts, the header first
type GuideRow = { name: string; code: string; zones: string[] };
type MatrixRow = { name: string; zones: string[]; services: { [header: string]: string[] } };

async function scrape(): Promise<ScrapeResult> {
  const [codes, guide, matrix] = await Promise.all([regionSet(), guideRows(), matrixRows()]);
  const products = assemble(codes, guide, matrix);
  const record = path.join(config.dataDir, `odb-regions-${new Date().toISOString().slice(0, 10)}.json`);
  fs.writeFileSync(record, JSON.stringify(products, null, 2));
  await upsertProducts(products);
  config.logger.info(`aws:odbRegions: ${products.length} regions -> ${path.basename(record)}`);
  return { failures: [] };
}

// ── the sources ─────────────────────────────────────────────────────────────

/** the regions the regional services table lists the service in; the -dup copies collapse on the set */
export async function regionSet(): Promise<Set<string>> {
  const resp = await axios.get<RegionalTable>(REGIONAL_TABLE_URL, { responseType: 'json', headers: { 'User-Agent': USER_AGENT } });
  const codes = new Set<string>();
  for (const p of resp.data.prices ?? []) {
    if (p.attributes?.['aws:serviceName'] === REGIONAL_TABLE_SERVICE && p.attributes['aws:region']) codes.add(p.attributes['aws:region']);
  }
  if (codes.size === 0) throw new Error(`aws:odbRegions: the regional services table lists no region for ${REGIONAL_TABLE_SERVICE}`);
  return codes;
}

/** every row of every guide table headed Region name / Region code / Availability Zones; a code seen in two tables keeps the union of its zones */
export async function guideRows(): Promise<Map<string, GuideRow>> {
  const html = await fetchHtml(AWS_GUIDE_URL);
  const rows = new Map<string, GuideRow>();
  for (const table of tablesOf(html)) {
    const [header, ...body] = table;
    if (header.length < 3 || !/region name/i.test(header[0]) || !/region code/i.test(header[1]) || !/availability zone/i.test(header[2])) continue;
    for (const cells of body) {
      const [name, code, zoneText] = cells;
      if (!code) continue;
      const zones = zoneText.match(ZONE_ID) ?? [];
      const seen = rows.get(code);
      rows.set(code, { name: seen?.name ?? name, code, zones: union(seen?.zones ?? [], zones) });
    }
  }
  if (rows.size === 0) throw new Error('aws:odbRegions: the AWS guide has no supported-regions table (the page changed)');
  return rows;
}

/** every live row of every matrix table: the region name, its zones, and the zones each service column grants */
export async function matrixRows(): Promise<MatrixRow[]> {
  const html = await fetchHtml(ORACLE_MATRIX_URL);
  const rows: MatrixRow[] = [];
  for (const table of tablesOf(html)) {
    const [header, ...body] = table;
    if (header.length < 4 || !/aws region/i.test(header[0]) || !/oci region/i.test(header[1]) || !/status/i.test(header[2])) continue;
    const services = header.slice(3);
    for (const cells of body) {
      if (cells.length < header.length) continue;
      if (!/^live$/i.test(cells[2].trim())) continue;
      const name = cells[0].split(':')[0].trim();
      const zones = cells[0].match(ZONE_ID) ?? [];
      const granted: { [header: string]: string[] } = {};
      services.forEach((service, i) => {
        granted[service] = grant(cells[3 + i], zones);
      });
      rows.push({ name, zones, services: granted });
    }
  }
  if (rows.length === 0) throw new Error("aws:odbRegions: Oracle's regional availability page has no matrix table (the page changed)");
  return rows;
}

/** a cell of the matrix: x = none; a tick with zone ids = those; a tick alone = every zone of the row */
function grant(cell: string, rowZones: string[]): string[] {
  const text = cell.trim();
  if (/^x$/i.test(text) || text === '') return [];
  const ids = text.match(ZONE_ID) ?? [];
  return ids.length ? ids : rowZones;
}

// ── the rows ────────────────────────────────────────────────────────────────

export function assemble(codes: Set<string>, guide: Map<string, GuideRow>, matrix: MatrixRow[]): Product[] {
  const guideCodes = new Set(guide.keys());
  const onlyInTable = [...codes].filter((c) => !guideCodes.has(c)).sort();
  const onlyInGuide = [...guideCodes].filter((c) => !codes.has(c)).sort();
  if (onlyInTable.length || onlyInGuide.length) {
    throw new Error(`aws:odbRegions: the regional services table and the AWS guide disagree on the region set (table only: ${onlyInTable.join(', ') || 'none'}; guide only: ${onlyInGuide.join(', ') || 'none'})`);
  }
  const byName = new Map<string, GuideRow>();
  const byZonePrefix = new Map<string, GuideRow>();
  for (const row of guide.values()) {
    byName.set(normalise(row.name), row);
    for (const zone of row.zones) byZonePrefix.set(zonePrefix(zone), row);
  }
  const products: Product[] = [];
  const matched = new Set<string>();
  for (const m of matrix) {
    const g = guideRowFor(m, byZonePrefix, byName);
    matched.add(g.code);
    const attributes: { [k: string]: string } = {
      regionCode: g.code,
      location: g.name,
      locationType: 'AWS Region',
      zones: union(g.zones, m.zones).join(','),
    };
    for (const [header, zones] of Object.entries(m.services)) attributes[`${SERVICE_ATTRIBUTE_PREFIX}${header}`] = zones.join(',');
    const product: Product = {
      productHash: '',
      sku: `odb:region:${g.code}`,
      vendorName: 'aws',
      region: g.code,
      service: 'AmazonODB',
      productFamily: 'Region availability',
      attributes,
      prices: [],
    };
    product.productHash = generateProductHash(product);
    products.push(product);
  }
  const unmatched = [...codes].filter((c) => !matched.has(c)).sort();
  if (unmatched.length) throw new Error(`aws:odbRegions: Oracle's matrix has no live row for ${unmatched.join(', ')}`);
  return products.sort((a, b) => a.region!.localeCompare(b.region!));
}

// ── helpers ─────────────────────────────────────────────────────────────────

async function fetchHtml(url: string): Promise<string> {
  const resp = await axios.get<string>(url, { responseType: 'text', headers: { 'User-Agent': USER_AGENT } });
  if (typeof resp.data !== 'string' || resp.data.length < 1000) throw new Error(`aws:odbRegions: ${url} answered no page`);
  return resp.data;
}

/** every table of a page as rows of cell texts: tags stripped, entities decoded, whitespace collapsed */
export function tablesOf(html: string): Table[] {
  const body = html.replace(/<(script|style)[^>]*>[\s\S]*?<\/\1>/gi, '');
  const tables: Table[] = [];
  for (const table of body.match(/<table[\s\S]*?<\/table>/gi) ?? []) {
    const rows: Table = [];
    for (const tr of table.match(/<tr[\s\S]*?<\/tr>/gi) ?? []) {
      const cells = (tr.match(/<t[hd][\s\S]*?<\/t[hd]>/gi) ?? []).map(cellText);
      if (cells.length) rows.push(cells);
    }
    if (rows.length) tables.push(rows);
  }
  return tables;
}

function cellText(cell: string): string {
  return cell
    .replace(/<br\s*\/?>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, ' ')
    .trim();
}

function normalise(name: string): string {
  return name.replace(/\s+/g, ' ').trim().toLowerCase();
}

/** the region part of a zone id: cac1-az1 → cac1; one prefix per region on both pages */
function zonePrefix(zone: string): string {
  return zone.split('-')[0];
}

/** the guide row of a matrix row: the one whose zone ids carry the row's zone-id prefix, else the one named as the row names it */
function guideRowFor(m: MatrixRow, byZonePrefix: Map<string, GuideRow>, byName: Map<string, GuideRow>): GuideRow {
  const prefixes = [...new Set(m.zones.map(zonePrefix))];
  if (prefixes.length > 1) throw new Error(`aws:odbRegions: Oracle's matrix row ${m.name} names zones of more than one region: ${m.zones.join(', ')}`);
  const g = (prefixes.length ? byZonePrefix.get(prefixes[0]) : undefined) ?? byName.get(normalise(m.name));
  if (!g) throw new Error(`aws:odbRegions: Oracle's matrix names a region the AWS guide does not, by zone id or by name: ${m.name} (${m.zones.join(', ') || 'no zone id'})`);
  return g;
}

function union(a: string[], b: string[]): string[] {
  return [...new Set([...a, ...b])].sort();
}

export default {
  scrape,
};
