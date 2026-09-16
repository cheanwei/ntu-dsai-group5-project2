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

## Power BI

1. Install Power BI Desktop (Windows).
2. Open the `.pbix` report file in Power BI Desktop.
3. Connect to the BigQuery marts dataset (Get Data → Google BigQuery) and
   refresh.
