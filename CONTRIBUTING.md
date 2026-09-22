# Development

**Prerequisites**:

- Node.js version >= 14.15.0
- Postgres version >= 13

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
   # Don't forget to run `CREATE DATABASE cloud_pricing;` first.
   POSTGRES_URI=postgresql://postgres:my_password@localhost:5432/cloud_pricing

   # The API key generated in step 2, used to authenticate clients calling this API.
   SELF_HOSTED_INFRACOST_API_KEY=<API Key from Step 2>
   ```

4. Install the npm packages:

   ```sh
   npm install
   ```

5. Download the pricing data, this can take a few minutes. The second command will show the number of products in the DB (each product has multiple prices).

   ```sh
   npm run job:init:dev && npm run data:status:dev
   ```

   If there are DB changes, run `npm run db:setup:dev` to apply them. The init job runs this too so this should only be needed if you haven't run that recently.

6. Prices can be kept up-to-date by running the update job once a week, for example from cron:

   ```sh
   npm run job:update:dev

   # Cron: add a cron job to run every week to update the database data. The cron entry should look something like:
   0 4 * * SUN npm run-script job:update >> /var/log/cron.log 2>&1
   ```

7. Start the server:

   In development mode:

   ```sh
   npm run dev
   ```

   In production mode:

   ```sh
   npm run build
   npm run start
   ```

   `curl -i http://localhost:4000/health` should show success.

8. Query the API at [http://localhost:4000/graphql](http://localhost:4000/graphql). Use the `X-Api-Key` HTTP header set to your `SELF_HOSTED_INFRACOST_API_KEY` — the [modheader](https://bewisse.com/modheader/) browser extension is handy for setting this in the GraphQL Playground.

# Release

1. In `package.json` update `version`, run `npm install` and push to master.
2. Run `git tag vx.y.z && git push origin vx.y.z`.
3. Wait for the GH Actions to complete as that creates/pushes the docker tag.
4. Go to the repository's **Releases** page, find the diff between the latest and the previous tag to write a description, then click **Draft New Release**, set `vx.y.z` as the tag name and title, add the description and publish.
5. Close addressed issues and tag anyone who liked/commented in them to tell them it's live in version X.
