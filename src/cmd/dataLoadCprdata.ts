import glob from 'glob';
import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { promisify } from 'util';
import { pipeline } from 'stream';
import { from as copyFrom } from 'pg-copy-streams';
import { PoolClient } from 'pg';
import yargs from 'yargs';
import ProgressBar from 'progress';
import format from 'pg-format';
import config from '../config';
import {
  truncateProductsTable,
} from '../db/setup';

async function run(): Promise<void> {
  const pool = await config.pg();

  const argv = await yargs
    .usage(
      'Usage: $0 --path=[ location of *.csv.gz files, default: ./data/cprdata ] --skip-data-load=[ skip data loading and only refresh views ]'
    )
    .options({
      path: { type: 'string', default: './data/cprdata' },
      'skip-data-load': { type: 'boolean', default: false, description: 'Skip data loading and only refresh materialized views' },
    }).argv;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    if (!argv['skip-data-load']) {
      await loadFiles(argv.path, client);
    } else {
      config.logger.info('Skipping data loading as requested');
    }

    await refreshViews(client);

    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');

    throw e;
  } finally {
    client.release();
  }
}

export async function refreshViews(client: PoolClient): Promise<void> {
  if (config.viewsToRefresh.length === 0) {
    config.logger.info('No views to refresh');
    return;
  }
  
  for (const viewName of config.viewsToRefresh) {
    config.logger.info(`Refreshing view: ${viewName}`);
    await client.query(`REFRESH MATERIALIZED VIEW ${viewName} WITH DATA`);
  }
}

async function loadFiles(dataPath: string, client: PoolClient): Promise<void> {
  const filenames = glob.sync(`${dataPath}/*.csv.gz`);
  if (filenames.length === 0) {
    config.logger.error(
      `Could not load data: There are no data files at '${dataPath}/*.csv.gz'`
    );
    config.logger.error(
      `The latest data files can be downloaded with "npm run-scripts data:download"`
    );
    process.exit(1);
  }

  // Group files by table name and truncate each table before loading
  const tableNames = new Set<string>();
  for (const filename of filenames) {
    const tableName = getTableNameFromFilename(filename);
    tableNames.add(tableName);
  }

  // Truncate all tables that will be loaded
  for (const tableName of tableNames) {
    config.logger.info(`Truncating table: ${tableName}`);
    if (tableName === config.productTableNameCprData) {
      await truncateProductsTable(client, tableName);
    } else {
      // For other tables, use generic truncate
      await client.query(format('TRUNCATE TABLE %I', tableName));
    }
  }

  // Load all files
  for (const filename of filenames) {
    const tableName = getTableNameFromFilename(filename);
    config.logger.info(`Loading file: ${filename} into table: ${tableName}`);
    await loadFile(client, filename, tableName);
  }
}

function getTableNameFromFilename(filename: string): string {
  const basename = path.basename(filename, '.csv.gz');
  return basename;
}

async function loadFile(client: PoolClient, filename: string, tableName: string): Promise<void> {
  const promisifiedPipeline = promisify(pipeline);

  const gunzip = zlib.createGunzip().on('error', (e) => {
    config.logger.error(
      `There was an error decompressing the file: ${e.message}`
    );
    config.logger.error(
      `The latest data files can be downloaded with "npm run-scripts data:download"`
    );
    process.exit(1);
  });

  // Use FORCE_NOT_NULL for producthash only if it's the main pricing table
  const forceNotNullClause = tableName === config.productTableNameCprData 
    ? ', FORCE_NOT_NULL ("producthash")' 
    : '';

  const pgCopy = client.query(
    copyFrom(format(`
    COPY %I FROM STDIN WITH (
      FORMAT csv,
      HEADER true,
      DELIMITER ','%s
    )`, tableName, forceNotNullClause))
  );

  const { size } = fs.statSync(filename);
  const progressBar = new ProgressBar(
    '-> loading [:bar] :percent (:etas remaining)',
    {
      width: 40,
      complete: '=',
      incomplete: ' ',
      renderThrottle: 500,
      total: size,
    }
  );

  const readStream = fs.createReadStream(filename);
  readStream.on('data', (buffer) => progressBar.tick(buffer.length));

  return promisifiedPipeline(readStream, gunzip, pgCopy);
}

config.logger.info('Starting: loading data into DB');
run()
  .then(() => {
    config.logger.info('Completed: loading data into DB');
    process.exit(0);
  })
  .catch((err) => {
    config.logger.error(err);
    process.exit(1);
  });
