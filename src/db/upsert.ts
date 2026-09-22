import format from 'pg-format';
import { Product, Price } from './types';
import config from '../config';
import fs from 'fs';
import path from 'path';

const batchSize = 1000;

const sqlLogPath = path.join(process.cwd(), 'data', 'sql_debug.log');

function logSql(label: string, sql: string): void {
  const entry = `-- [${new Date().toISOString()}] ${label}\n${sql}\n\n`;
  fs.appendFileSync(sqlLogPath, entry, 'utf8');
}

async function upsertProducts(products: Product[]): Promise<number> {
  const pool = await config.pg();
  config.logger.info(`Upserting ${products.length} products`);

  const insertSql = format(
    `INSERT INTO %I ("productHash", "sku", "vendorName", "region", "service", "productFamily", "attributes", "prices") VALUES `,
    config.productTableName
  );

  const onConflictSql = format(
    ` 
    ON CONFLICT ("productHash") DO UPDATE SET
    "sku" = excluded."sku",
    "vendorName" = excluded."vendorName",
    "region" = excluded."region",
    "service" = excluded."service",
    "productFamily" = excluded."productFamily",
    "attributes" = excluded."attributes",
    "prices" = %I."prices" || excluded."prices"        
    `,
    config.productTableName
  );

  let totalRows = 0;
  const productHashToInsertRow: Map<string, string> = new Map();

  const flushBatch = async () => {
    if (productHashToInsertRow.size === 0) return;
    const sql =
      insertSql +
      Array.from(productHashToInsertRow.values()).join(',') +
      onConflictSql;
    const result = await pool.query(sql);
    totalRows += result.rowCount ?? 0;
    productHashToInsertRow.clear();
  };

  for (const product of products) {
    if (
      productHashToInsertRow.size > batchSize ||
      productHashToInsertRow.has(product.productHash)
    ) {
      await flushBatch();
    }

    const pricesMap: { [priceHash: string]: Price[] } = {};
    product.prices.forEach((price) => {
      if (pricesMap[price.priceHash]) {
        pricesMap[price.priceHash].push(price);
      } else {
        pricesMap[price.priceHash] = [price];
      }
    });

    productHashToInsertRow.set(
      product.productHash,
      format(
        `(%L, %L, %L, %L, %L, %L, %L, %L)`,
        product.productHash,
        product.sku,
        product.vendorName,
        product.region,
        product.service,
        product.productFamily || '',
        product.attributes,
        pricesMap
      )
    );
  }

  await flushBatch();
  config.logger.info(`upsertProducts: ${totalRows} rows affected`);
  return totalRows;
}

/**
 * Merge savings-plan prices into existing product rows.
 *
 * Uses a pure UPDATE (no INSERT). Standard pricing files must have been loaded
 * first so the product rows already exist. If a productHash is not found in the
 * table the UPDATE simply matches 0 rows and the price is skipped — this is
 * intentional: we never want stub rows with empty attributes in the table.
 */
async function upsertProductPricesOnly(products: Product[]): Promise<number> {
  if (products.length === 0) return 0;

  const pool = await config.pg();
  config.logger.info(`Merging savings-plan prices for ${products.length} products (UPDATE-only strategy)`);

  const entries: [string, { [priceHash: string]: Price[] }][] = products.map((product) => {
    const pricesMap: { [priceHash: string]: Price[] } = {};
    product.prices.forEach((price) => {
      if (pricesMap[price.priceHash]) {
        pricesMap[price.priceHash].push(price);
      } else {
        pricesMap[price.priceHash] = [price];
      }
    });
    return [product.productHash, pricesMap];
  });

  let totalRows = 0;

  for (let i = 0; i < entries.length; i += batchSize) {
    const chunk = entries.slice(i, i + batchSize);

    const valuesSql = chunk
      .map(([productHash, pricesMap]) => format(`(%L, %L::jsonb)`, productHash, JSON.stringify(pricesMap)))
      .join(',');

    const sql = format(
      `UPDATE %I AS target
       SET "prices" = target."prices" || source.prices
       FROM (VALUES %s) AS source("productHash", prices)
       WHERE target."productHash" = source."productHash"`,
      config.productTableName,
      valuesSql
    );

    const result = await pool.query(sql);
    const rowCount = result.rowCount ?? 0;
    totalRows += rowCount;
    config.logger.info(`  batch: ${chunk.length} attempted, ${rowCount} rows updated`);
    if (rowCount < chunk.length) {
      config.logger.warn(`  ${chunk.length - rowCount} productHash(es) not found in table — standard pricing may not have been loaded for this region yet`);
    }
  }

  config.logger.info(`upsertProductPricesOnly: ${totalRows} rows updated in total out of ${products.length} attempted`);
  return totalRows;
}

async function upsertPrice(product: Product, price: Price): Promise<void> {
  const pool = await config.pg();

  await pool.query(
    format(
      `UPDATE %I SET "prices" = "prices" || %L WHERE "productHash" = %L`,
      config.productTableName,
      { [price.priceHash]: [price] },
      product.productHash
    )
  );
}

async function upsertPrices(pricesByProductHash: Map<string, Price[]>): Promise<void> {
  if (pricesByProductHash.size === 0) {
    return;
  }

  const pool = await config.pg();
  const entries = Array.from(pricesByProductHash.entries());

  for (let i = 0; i < entries.length; i += batchSize) {
    const chunk = entries.slice(i, i + batchSize);
    const valuesSql = chunk
      .map(([productHash, prices]) => {
        const pricesMap: { [priceHash: string]: Price[] } = {};
        prices.forEach((price) => {
          if (pricesMap[price.priceHash]) {
            pricesMap[price.priceHash].push(price);
          } else {
            pricesMap[price.priceHash] = [price];
          }
        });

        return format(`(%L, %L)`, productHash, pricesMap);
      })
      .join(',');

    const updateSql = format(
      `UPDATE %I AS target
       SET "prices" = target."prices" || source."prices"
       FROM (VALUES %s) AS source("productHash", "prices")
       WHERE target."productHash" = source."productHash"`,
      config.productTableName,
      valuesSql
    );

    await pool.query(updateSql);
  }
}

export { upsertProducts, upsertProductPricesOnly, upsertPrice, upsertPrices };
