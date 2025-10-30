import { PoolClient } from 'pg';
import config from '../config';
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