import fs from 'fs';
import zlib from 'zlib';
import { to as copyTo } from 'pg-copy-streams';
import yargs from 'yargs';
import { PoolClient } from 'pg';
import { promisify } from 'util';
import { pipeline } from 'stream';
import format from 'pg-format';
import config from '../config';

async function run(): Promise<void> {
  const argv = await yargs
    .usage(
      'Usage: $0 --out=[output directory, default: ./data/cprdata] --table=[table name, default: aws_ec2_prices]'
    )
    .options({
      out: { type: 'string', default: './data/cprdata' },
      table: { type: 'string', default: 'aws_ec2_prices' },
    }).argv;

  const pool = await config.pg();
  const client = await pool.connect();
  try {
    const outputFile = `${argv.out}/${argv.table}.csv.gz`;
    config.logger.info(`Dumping table '${argv.table}' to file: ${outputFile}`);
    await dumpFile(client, outputFile, argv.table);
  } finally {
    client.release();
  }
}

async function dumpFile(client: PoolClient, outfile: string, tableName: string): Promise<void> {
  const promisifiedPipeline = promisify(pipeline);

  const copyStream = client.query(
    copyTo(
      format(
        `
      COPY (SELECT * FROM %I) TO STDOUT WITH (
      FORMAT csv,
      HEADER true,
      DELIMITER ','
    )`,
        tableName
      )
    )
  );

  const gzip = zlib.createGzip().on('error', (e) => {
    config.logger.info(e);
    process.exit(1);
  });

  const fileStream = fs.createWriteStream(`${outfile}`);

  return promisifiedPipeline(copyStream, gzip, fileStream);
}

config.logger.info('Starting: creating dump of DB data');
run()
  .then(() => {
    config.logger.info('Completed: creating dump of DB data');
    process.exit(0);
  })
  .catch((err) => {
    config.logger.error(err);
    process.exit(1);
  });
