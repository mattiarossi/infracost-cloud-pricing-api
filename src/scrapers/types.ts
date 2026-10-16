/** a file a scraper could not load; it carries no load marker, so the next run loads it again */
export interface FileFailure {
  file: string;
  error: string;
}

/**
 * what a scraper's scrape() may return: at least the files it could not load. awsBulk returns its run
 * statistics, which carry them; a scraper that returns nothing reports no failed file.
 */
export interface ScrapeResult {
  failures: FileFailure[];
}
