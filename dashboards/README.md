# Dashboard setup

Two dashboards sit over the dbt marts in BigQuery: a Streamlit app in this
folder, and a Power BI report authored outside the repo.

## Prerequisites

- Dependencies installed from the repo root: `uv sync` (Flask, SQLAlchemy,
  sqlalchemy-bigquery, Streamlit, Folium and Plotly are all declared in
  `pyproject.toml`).
- A `.env` at the repo root (copy `.env.example`) with:
  - `GCP_PROJECT` — the BigQuery project (defaults to `dsai6mod2`)
  - `BIGQUERY_MARTS_DATASET` — the marts dataset (defaults to `dbt_dev_marts`)
  - `GOOGLE_APPLICATION_CREDENTIALS` — path to a service-account key, or run
    `gcloud auth application-default login` instead
  - `TOP_CITIES_LIMIT` — optional, cities shown on the map (default `10`)
- The marts (`dim_customer`, `dim_product`, `fct_order_items`) built by dbt.

## Streamlit dashboard

Start two processes from the repo root, in this order.

**1. Cities API** (Flask, port 5001) — keep this terminal running:

```sh
uv run python dashboards/citiesapi.py
```

Check it with `curl http://localhost:5001/api/cities`; it should return JSON
with a `cities` list.

**2. Streamlit app**, in a second terminal:

```sh
uv run streamlit run dashboards/dashboard.py
```

| Page | File | Data source |
| --- | --- | --- |
| Olist Revenue Geo-Map | `streamlit/salesmap.py` | Flask `/api/cities` |
| Top Product Revenue | `streamlit/sunburst.py` | Direct SQL to BigQuery |

## Deploy to Google Cloud Run

One container runs both processes. `start.sh` starts the cities API on
`127.0.0.1:5001` (not exposed) and Streamlit on the port Cloud Run assigns.
The image installs `dashboards/requirements.txt` only, not the full `uv.lock`.
There is no key file: the service runs as a dedicated service account, and
BigQuery picks up its credentials from the metadata server.

**One-time setup** (`P` is the GCP project id):

```sh
P=your-gcp-project-id   # the project holding olist_marts, same as GCP_PROJECT in .env
gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com --project $P
gcloud iam service-accounts create olist-dashboard --project $P
for r in roles/bigquery.dataViewer roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding $P --condition=None \
    --member=serviceAccount:olist-dashboard@$P.iam.gserviceaccount.com --role=$r
done
```

**Deploy** from the repo root (re-run to ship an update):

```sh
gcloud run deploy olist-dashboard --source dashboards \
  --project $P --region us-central1 \
  --service-account olist-dashboard@$P.iam.gserviceaccount.com \
  --set-env-vars GCP_PROJECT=$P,BIGQUERY_MARTS_DATASET=olist_marts,TOP_CITIES_LIMIT=10 \
  --allow-unauthenticated --session-affinity --timeout 3600 \
  --memory 1Gi --cpu 1 --min-instances 0 --max-instances 2
```

- `--session-affinity` and `--timeout 3600` are there because Streamlit keeps a
  WebSocket open for each browser session.
- `--min-instances 0` scales to zero when idle, so an idle app costs nothing.
  The first visit after that waits a few seconds for a cold start.
- If the build fails on permissions, grant the build service account named in
  the error `roles/run.builder`.
- Logs: `gcloud run services logs read olist-dashboard --region us-central1 --project $P`.

**Test the image locally** using your own gcloud credentials:

```sh
docker build -t olist-dashboard dashboards
docker run --rm -p 8080:8080 -e GCP_PROJECT=$P \
  -e BIGQUERY_MARTS_DATASET=olist_marts \
  -v ~/.config/gcloud:/root/.config/gcloud:ro olist-dashboard
# open http://localhost:8080
```

### Sharing with people who have no GCP access

Viewers never need GCP access. Their browser only talks to Streamlit, and every
BigQuery query runs inside the container as `olist-dashboard`. The
`--allow-unauthenticated` flag makes the service URL public, so send people the
`https://olist-dashboard-….run.app` link that `gcloud run deploy` prints.

- If the deploy fails with an error that `allUsers` is not allowed, the project
  has an org policy (domain-restricted sharing) blocking public services. Deploy
  in a personal project instead, or ask the org admin for an exception.
- To restrict access without granting GCP roles, add a password check in
  `dashboard.py`, or use Streamlit's `st.login()` with a Google OAuth client.
- `--max-instances 2` and the one-hour `st.cache_data` TTL limit how many
  BigQuery queries anonymous traffic can trigger.

## Power BI

1. Install Power BI Desktop (Windows).
2. Open the `.pbix` report file in Power BI Desktop.
3. Connect to the BigQuery marts dataset (Get Data → Google BigQuery) and
   refresh.
