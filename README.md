# Cloud Pricing API

The Cloud Pricing API is a self-hosted GraphQL-based API that includes all public prices from AWS, Azure and Google; it contains over **3 million prices!** Pricing data is fetched directly from cloud vendor public pricing endpoints and kept up-to-date via a weekly job.

## Usage

Once deployed, query the local GraphQL endpoint. The following example `curl` fetches the latest on-demand price for an AWS EC2 m3.large instance in us-east-1. More examples can be found in `./examples/queries`.

**Example request**:
```sh
curl http://localhost:4000/graphql \
  -X POST \
  -H 'X-Api-Key: YOUR_API_KEY_HERE' \
  -H 'Content-Type: application/json' \
  --data '
  {"query": "{ products(filter: {vendorName: \"aws\", service: \"AmazonEC2\", region: \"us-east-1\", attributeFilters: [{key: \"instanceType\", value: \"m3.large\"}, {key: \"operatingSystem\", value: \"Linux\"}, {key: \"tenancy\", value: \"Shared\"}, {key: \"capacitystatus\", value: \"Used\"}, {key: \"preInstalledSw\", value: \"NA\"}]}) { prices(filter: {purchaseOption: \"on_demand\"}) { USD } } } "}
  '
```

**Example response**:
```sh
{"data":{"products":[{"prices":[{"USD":"0.1330000000"}]}]}}
```

The GraphQL Playground is available at [http://localhost:4000/graphql](http://localhost:4000/graphql).

## Architecture

The following diagram shows an overview of the architecture.

![Deployment overview](.github/assets/deployment_overview.png "Deployment overview")

Pricing data is fetched directly from cloud vendor public pricing endpoints (e.g. `https://pricing.us-east-1.amazonaws.com` for AWS). A weekly job downloads the latest pricing files and upserts them into the local PostgreSQL database. Already-downloaded and already-loaded files are automatically skipped so repeated runs are fast and idempotent.

## Deployment

It should take around 15 mins to deploy the Cloud Pricing API. Two deployment methods are supported:
1. If you have a Kubernetes cluster, we recommend using [our Helm Chart](https://github.com/infracost/helm-charts/tree/master/charts/cloud-pricing-api).
2. If you prefer to deploy in a VM, we recommend using [Docker compose](#docker-compose).

The Cloud Pricing API includes an unauthenticated `/health` path that is used by the Helm chart and Docker compose deployments.

The PostgreSQL DB is run on a single container/pod by default, which should be fine if your high-availability requirements allow for a few second downtime on container/pod restarts. No critical data is stored in the DB and the DB can be quickly recreated in the unlikely event of data corruption issues. Managed databases, such as a small AWS RDS or Azure Database for PostgreSQL, can also be used (PostgreSQL version >= 13). Since the pricing data can be quickly populated by running the update job, you can probably start without a backup strategy.

### Helm chart

See [our Helm Chart](https://github.com/infracost/helm-charts/tree/master/charts/cloud-pricing-api) for details.

### Docker compose

#### Prerequisites

* Docker Engine 17.09.0+

#### Steps

1. Clone the repo:

    ```sh
    git clone https://github.com/mattiarossi/infracost-cloud-pricing-api
    cd cloud-pricing-api
    ```

2. Generate a 32 character API token that clients will use to authenticate against this API. If you ever need to rotate the key, update this variable and restart the application.

    ```sh
    export SELF_HOSTED_INFRACOST_API_KEY=$(cat /dev/urandom | env LC_CTYPE=C tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
    echo "SELF_HOSTED_INFRACOST_API_KEY=$SELF_HOSTED_INFRACOST_API_KEY"
    ```

3. Add a `.env` file with the following content:

    ```sh
    # The API key generated in step 2, used to authenticate clients calling this API.
    SELF_HOSTED_INFRACOST_API_KEY=<API Key from Step 2>
    ```

4. Run `docker-compose run init_job`. This will start a PostgreSQL DB container and an init container that downloads pricing data directly from AWS/Azure/GCP public endpoints and loads it into the DB. The init container will take a few minutes and exit after the Docker compose logs show `Completed: loading data into DB`.

5. Run `docker-compose up api`. This will start the Cloud Pricing API.

6. Prices can be kept up-to-date by running the update job once a week, for example from cron:

    ```sh
    # Add a weekly cron job to update the pricing data. The cron entry should look something like:
    0 4 * * SUN docker-compose run --rm update_job npm run job:update >> /var/log/cron.log 2>&1
    ```

7. Point your API client to `http://localhost:4000` (or your deployed endpoint) and authenticate using your `SELF_HOSTED_INFRACOST_API_KEY`.

8. The home page for the Cloud Pricing API, [http://localhost:4000](http://localhost:4000), shows if prices are up-to-date and some statistics.

![Stats page](.github/assets/stats_page.png "Stats page")

We recommend you set up a subdomain (and TLS certificate) to expose the API to your clients.

You can also access the GraphQL Playground at [http://localhost:4000/graphql](http://localhost:4000/graphql) using something like the [modheader](https://bewisse.com/modheader/) browser extension to set the `X-Api-Key` header to your `SELF_HOSTED_INFRACOST_API_KEY`.

To upgrade to the latest version, run `docker-compose pull` followed by `docker-compose up`.

The environment variable `DISABLE_TELEMETRY` can be set to `true` to opt-out of telemetry.

## Environment Variables

The following environment variables can be set in your `.env` file (or passed directly to the container):

| Variable | Default | Description |
|---|---|---|
| `SELF_HOSTED_INFRACOST_API_KEY` | *(required)* | API key used to authenticate clients calling this API. |
| `POSTGRES_URI` | — | Full PostgreSQL connection URI. Takes precedence over the individual `POSTGRES_*` variables below. |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host. |
| `POSTGRES_PORT` | `5432` | PostgreSQL port. |
| `POSTGRES_USER` | `postgres` | PostgreSQL user. |
| `POSTGRES_PASSWORD` | *(empty)* | PostgreSQL password. |
| `POSTGRES_DB` | `cloud_pricing` | PostgreSQL database name. |
| `POSTGRES_CREDENTIALS` | — | JSON credentials blob for managed PostgreSQL instances. When set, individual `POSTGRES_*` variables are ignored. See [`POSTGRES_CREDENTIALS` format](#postgres_credentials-format) for the expected structure. |
| `INFRACOST_PRICING_API_ENDPOINT` | `https://pricing.api.github.io` | Base URL of the pricing API that the service exposes and refers to internally. Override this when running behind a reverse proxy or when pointing the API at a different upstream. |
| `INFRACOST_DASHBOARD_API_ENDPOINT` | `https://dashboard.api.github.io` | Base URL of the dashboard/telemetry API. Override to redirect dashboard calls to a different endpoint (e.g. an internal proxy). |
| `DISABLE_TELEMETRY` | `false` | Set to `true` to opt-out of usage telemetry. |
| `PORT` | `4000` | Port the API listens on. |
| `GCP_API_KEY` | — | Google Cloud API key for fetching GCP pricing. |
| `GCP_KEY_FILE` | — | Path to a GCP service-account JSON key file. |
| `GCP_KEY_FILE_CONTENT` | — | Raw JSON content of a GCP service-account key (used when a file path is impractical, e.g. in containers). |
| `GCP_PROJECT` | — | GCP project ID. |
| `IBM_CLOUD_API_KEY` | — | IBM Cloud API key for fetching IBM pricing. |
| `VIEWS_TO_REFRESH` | — | Comma-separated list of PostgreSQL views to refresh after a pricing update. |

### `POSTGRES_CREDENTIALS` format

`POSTGRES_CREDENTIALS` accepts a single JSON string containing full connection details for a managed PostgreSQL instance. When this variable is set, all individual `POSTGRES_*` variables are ignored.

Expected JSON structure:

```json
{
  "connection": {
    "postgres": {
      "authentication": {
        "method": "direct",
        "username": "db_user",
        "password": "s3cr3t"
      },
      "certificate": {
        "name": "my-cert",
        "certificate_authority": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n",
        "certificate_base64": "<base64-encoded PEM certificate>"
      },
      "composed": [
        "postgresql://db_user:s3cr3t@db.example.com:5432/mydb?sslmode=verify-full"
      ],
      "database": "mydb",
      "hosts": [
        { "hostname": "db.example.com", "port": 5432 }
      ],
      "path": "myschema",
      "path_views": "myschema_views",
      "query_options": {
        "sslmode": "verify-full"
      },
      "scheme": "postgresql",
      "type": "uri",
      "view_refresh": ["materialized_view_one", "materialized_view_two"]
    }
  },
  "instance_administration_api": {
    "deployment_id": "my-deployment-id",
    "instance_id": "my-instance-id",
    "root": "https://api.example.com/v5/postgres"
  }
}
```

| Field | Used as |
|---|---|
| `connection.postgres.authentication.username` | DB user |
| `connection.postgres.authentication.password` | DB password |
| `connection.postgres.hosts[0].hostname` | DB host |
| `connection.postgres.hosts[0].port` | DB port |
| `connection.postgres.database` | DB name (falls back to `POSTGRES_DB`) |
| `connection.postgres.certificate.certificate_base64` | Base64-encoded TLS CA certificate for the SSL connection |
| `connection.postgres.path` | PostgreSQL `search_path` schema |
| `connection.postgres.path_views` | Secondary schema appended to `search_path` |
| `connection.postgres.view_refresh` | Overrides `VIEWS_TO_REFRESH` |

To pass the credentials as an environment variable, serialize the JSON to a single line:

```sh
export POSTGRES_CREDENTIALS=$(cat credentials.json | jq -c .)
```

## Troubleshooting

For issues, please open a GitHub issue in this repository.

## AWS Bulk Scraper (`awsBulk`)

The `awsBulk` scraper downloads and ingests AWS public pricing data, including standard on-demand/reserved pricing and the newer **Savings Plans** pricing. It is located at `src/scrapers/awsBulk.ts`.

### File naming conventions

Downloaded files are stored in the `data/` directory and include the **publication timestamp** of the AWS pricing release they hold. For a region-split service and for a savings plan the timestamp is the version segment of the regional URL. For a single-file service — whose `currentVersionUrl` is `/current/index.json` and carries none — it is `currentVersion` from the offer's version index (`versionIndexUrl`), and the file is fetched from that publication's own URL. This ensures that a new AWS pricing release produces a new file name and triggers a fresh download and load, without requiring a manual cleanup; a file name without a timestamp is refused.

| Type | Example filename |
|---|---|
| Single-file service | `data/aws-AmazonRDS-20260224205727.json` |
| Region-split service (e.g. EC2) | `data/aws-AmazonEC2-us-east-1-20260224205727.json` |
| Savings Plan | `data/aws-AWSComputeSavingsPlan-savings-plan-us-east-1-20260224214300.json` |

When a new version is downloaded, any previously-downloaded file for the same service/region with a different timestamp is automatically deleted, as is a `-current.json` file (the version-less name earlier releases of the scraper wrote for every single-file service) together with its `.loaded` marker.

### Load markers (`.loaded` sentinel files)

After a file has been successfully parsed and upserted into the DB, a zero-byte `<filename>.json.loaded` marker is written alongside it. On subsequent runs, any file with an existing `.loaded` marker is skipped entirely — no parsing or DB round-trip occurs. Old markers are removed automatically when their corresponding data file is cleaned up.

### Savings Plans pricing

For AWS services that support Savings Plans (e.g. EC2, RDS, EKS, Lambda), `currentSavingsPlanIndexUrl` is populated in the AWS offers index. The scraper:

1. Fetches the region index from `currentSavingsPlanIndexUrl`.
2. Downloads one file per region, naming it with the `-savings-plan-<regionCode>-<version>` pattern.
3. Detects the savings plan format at parse time (the `terms.savingsPlan` array) and routes to a dedicated parser.
4. For each rate in a savings plan term, reconstructs the target product by `discountedSku` and merges a new `savings_plan` price entry onto the existing product row using `UPDATE … SET "prices" = "prices" || …` rather than a full upsert.

Savings plan prices carry extra fields on the `Price` object: `savingsPlanSku`, `discountedSku`, `discountedUsageType`, `discountedOperation`, `discountedServiceCode`, `discountedRegionCode`, `discountedInstanceType`, and `rateCode`.

### CLI flags

The scraper accepts the following command-line flags to control which files are (re-)loaded on a given run. These are especially useful after a partial failure or when you need to force-reload a subset of the data.

| Flag | Description |
|---|---|
| `--force` / `-f` | Re-process **all** files, ignoring `.loaded` markers |
| `--only-standard` | Process only **standard** pricing files (on-demand / reserved); savings plan files are skipped |
| `--only-savings-plan` | Process only **savings plan** files; standard pricing files are skipped |
| `--services <list>` / `-s <list>` | Comma-separated list of AWS offer codes to process; all others are skipped |

The `--services` filter is matched case-insensitively against the offer code embedded in the filename (e.g. `AmazonEC2`, `AmazonRDS`, `AWSComputeSavingsPlan`).

#### Examples

```sh
# Normal incremental run — skip anything already loaded
npm run job:update

# Force reload everything
npm run job:update -- --force

# Process only savings plan files (skip standard)
npm run job:update -- --only-savings-plan

# Load only RDS (standard + savings plan)
npm run job:update -- --services AmazonRDS,AWSDatabaseSavingsPlans

# Process only EC2 standard pricing for all regions
npm run job:update -- --only-standard --services AmazonEC2

# Process only the Compute Savings Plan data
npm run job:update -- --only-savings-plan --services AWSComputeSavingsPlan
```

---

## Contributing

Issues and pull requests are welcome! For development details, see the [contributing guide](CONTRIBUTING.md). For major changes, including interface changes, please open an issue first to discuss what you would like to change.

## License

[Apache License 2.0](https://choosealicense.com/licenses/apache-2.0/)
