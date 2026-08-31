# Olist Data Platform

End-to-end data platform over the [Olist Brazilian E-Commerce
dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce): nine
CSVs, ~120 MB, ~100k orders from September 2016 to October 2018.

```
Kaggle CSVs → GCS raw zone → dlt → BigQuery → dbt (staging → intermediate → marts)
                                       ↓                        ↓
                              Great Expectations        SQLAlchemy + pandas
                                                          (Jupyter notebooks)

              all of it orchestrated as Dagster software-defined assets
```

The full argument for every choice below — and for the ones rejected — is in
**[docs/architecture-design.md](docs/architecture-design.md)**. This README
covers setup and running only.

## Reports

| Report | URL |
|---|---|
| dbt docs — catalog and lineage | TODO: `https://<org>.github.io/<repo>/dbt/` |
| Great Expectations Data Docs | TODO: `https://<org>.github.io/<repo>/quality/` |
| Pipeline runs | GitHub Actions tab |

## Setup

Prerequisites: Python 3.11+, [uv](https://docs.astral.sh/uv/), a GCP project
with BigQuery enabled, and a Kaggle API token.

```bash
git clone <repo> && cd <repo>
uv sync                          # add --extra duckdb for the §12 fallback target
cp .env.example .env             # then fill it in
uv run nbstripout --install      # EVERY person, EVERY clone — see below

# Mints a Kaggle token (30-day) and writes it plus your personal dbt dataset
# into .env. Needs a real terminal for the browser approval step:
uv run python scripts/bootstrap_env.py --name <your-short-name>
# Already have a token from kaggle.com/settings? Put it in a file and point at it
# — never on the command line or after `echo`, where it lands in shell history:
uv run python scripts/bootstrap_env.py --name <your-short-name> --token-file ~/tok.txt
uv run dbt deps --project-dir transform
```

**Everything is US multi-region and that is immutable** (§14). Create the raw
bucket with `--location=US`; a bucket outside the US fails the load into a US
dataset with an error that appears to blame the bucket.

```bash
gcloud storage buckets create "gs://olist-raw-${GCP_PROJECT}" --location=US
bq --location=US mk --dataset "${GCP_PROJECT}:olist_raw"
bq --location=US mk --dataset "${GCP_PROJECT}:olist_marts"
bq --location=US mk --dataset "${GCP_PROJECT}:${DBT_DEV_DATASET}"
```

**The service account needs the bucket as well as BigQuery.** A key minted for
BigQuery has no storage role, and the failure comes late — the Kaggle download
succeeds, then the first upload returns a 403 naming `storage.objects.create`.
Grant it on the bucket, not the project, so the account stays scoped to the raw
zone:

```bash
gcloud storage buckets add-iam-policy-binding "gs://olist-raw-${GCP_PROJECT}" \
  --member="serviceAccount:<sa>@${GCP_PROJECT}.iam.gserviceaccount.com" \
  --role=roles/storage.objectUser
```

`objectUser` and not `objectCreator`: the upload needs create, and dlt needs get
and list to read those same CSVs back during the load.

**The service-account key never enters git.** `.gitignore` excludes `*.json`
from the first commit. In CI the key is the `GCP_SA_KEY` secret, written to
disk by the workflow and deleted in the same job.

## Running it

**Nothing loads `.env` for you.** No module calls `load_dotenv` yet, and the
pipeline reads `GCP_RAW_BUCKET`, `GOOGLE_APPLICATION_CREDENTIALS` and
`KAGGLE_API_TOKEN` from the process environment. Export them first, in the same
shell:

```bash
set -a; source .env; set +a
```

### The full pipeline, end to end

Two commands from a filled-in `.env` to populated marts:

```bash
uv run python orchestration/run_all.py                   # Kaggle → GCS → olist_raw
uv run dbt build --project-dir transform --target dev    # olist_raw → staging → intermediate → marts
```

The first materialises the whole Dagster asset graph in one process — the same
entrypoint GitHub Actions runs daily at 08:00 SGT. Budget roughly five minutes
cold, nearly all of it the 126 MB Kaggle download; afterwards `data/staging/`
and the raw-zone prefix are both populated and a re-run is much faster. The
second builds and tests the 21 dbt models.

**dbt is a separate command because it is not in the asset graph yet**
(`TODO(A1)` in `orchestration/assets.py`); Great Expectations (`TODO(B1)`) is
the same story. When both land, `run_all.py` covers the whole thing and the
second command goes away. Until then the two halves share a warehouse, not a
run: nothing stops you from building dbt against a raw zone that failed to
load, so check the first command exited 0.

Confirm what actually landed:

```bash
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM \`${GCP_PROJECT}.olist_raw.olist_orders_dataset\`"
```

About 99k orders. If it returns zero rows or the table is missing, the load did
not reach BigQuery — read the Dagster output rather than re-running blind.

To run it the way CI does, without waiting for 08:00: the **pipeline** workflow
has `workflow_dispatch`, so the Actions tab can trigger it on demand. That path
also generates dbt docs and publishes the reports to Pages.

The sections below are the same pipeline broken into pieces — use them when
iterating on one stage rather than running the lot.

### Ingestion — Kaggle → GCS → BigQuery (`olist_raw`)

```bash
uv run python -m scripts.run_ingestion --dry-run   # what would load, without loading
uv run python -m scripts.run_ingestion             # download, upload, load
```

`--bucket` defaults to `$GCP_RAW_BUCKET`, and `--ingest-date` to today. The date
is the raw-zone prefix: re-using one overwrites that prefix in place, a new one
lands beside it, and the load is `replace` either way — so re-running never
duplicates rows.

Roughly five minutes on a first run, nearly all of it the Kaggle download.
Afterwards kagglehub serves from its own cache, but the download and upload are
still the slow half, so while iterating on the load skip them entirely:

```bash
uv run python -m scripts.run_ingestion --bucket-url gs://<bucket>/2026-08-29
```

**What loads, and how, is `ingestion/config.yml`** — the pinned Kaggle version,
the nine tables, and the columns whose types are declared rather than inferred.
Read it before changing anything in `ingestion/*.py`; most changes belong in the
YAML, and `--dry-run` prints exactly what it resolves to.

Two failures are expected behaviour rather than bugs: an unexpected column fails
the load (the frozen schema contract, §4), and a missing source file fails the
download naming the file (the version pin no longer matching what Kaggle
serves). Both are meant to stop the run.

```bash
uv run pytest                    # 51 tests, no credentials or network needed
```

#### If the load fails with `CERTIFICATE_VERIFY_FAILED`

Only on the python.org macOS builds (`/Library/Frameworks/Python.framework/…`),
and only at the *load* step — the Kaggle download and the GCS upload succeed
first, which makes it look like a bucket problem. It is not.

Two HTTP stacks reach the same bucket. `google-cloud-storage` uses `requests`,
which bundles certifi and passes it explicitly, so the upload works. dlt's
filesystem source reads through `gcsfs` → `aiohttp`, which builds a default SSL
context and asks OpenSSL — and these builds ship with the CA store unwired.
Confirm it in one line:

```bash
uv run python -c "import ssl; print(ssl.get_default_verify_paths().cafile)"   # None → this is it
```

Fix the interpreter once. Existing venvs inherit it too — they resolve SSL
paths through the framework they were built from, so there is nothing to
rebuild:

```bash
"/Applications/Python 3.11/Install Certificates.command"
```

Quoted, not backslash-escaped: the path has two spaces in it and a stray
escape sends the shell looking for `/Applications/Python`. Re-run the check
above to confirm — it should now print a path ending `etc/openssl/cert.pem`.

Then resume without re-downloading — the raw zone is already populated:

```bash
uv run python -m scripts.run_ingestion --bucket-url gs://<bucket>/<ingest-date>
```

### Orchestration

The ingestion half of the graph is wired: `kaggle_dataset` → `gcs_raw_files` →
nine `olist_raw/<table>` assets, one per source table. The dbt models
(`TODO(A1)`) and the GX asset checks (`TODO(B1)`) join the same graph next.

```bash
# Development and demo — webserver + daemon on localhost:3000.
# The asset graph here is the strongest visual for the Technical Overview slide.
uv run dagster dev -m orchestration.definitions

# The whole graph, in-process.
uv run python orchestration/run_all.py
```

### dbt

Against your personal dataset so concurrent work never collides in the shared
marts:

```bash
cd transform
uv run dbt build --target dev          # run + test, staging → intermediate → marts
uv run dbt docs generate && uv run dbt docs serve
```

## Repository layout

| Path | Contents | Lane |
|---|---|---|
| `ingestion/` | Kaggle → GCS → dlt → `olist_raw` (§4) | A1 |
| `scripts/` | Run by hand: setup, and the manual ingestion entrypoint | A1 |
| `transform/` | dbt project: 9 staging, 4 intermediate, 8 marts (§5) | A2 |
| `quality/` | Great Expectations suites and Data Docs (§7) | B1 |
| `orchestration/` | Dagster assets, resources, schedule, CI entrypoint (§8) | A1 |
| `orchestration/deploy/` | Compose deployment that runs the graph — stretch, week 3 only (§8) | A1 |
| `notebooks/` | Four analyses over the marts (§9) | B2 |
| `docs/` | Architecture design and diagrams | C |
| `.github/workflows/` | Nightly run, artifacts, Pages deploy (§8) | A1 |

## Working agreements

These prevent the failures that six people in one repo reliably produce (§10):

- **`nbstripout --install`, run by every person on every clone.** `.gitattributes`
  is committed, so the *rule* (`*.ipynb filter=nbstripout`) arrives with the
  clone — but the filter itself is registered in `.git/config`, which is not
  cloned. A teammate who skips this gets no error: git treats an unregistered
  filter as a pass-through and they quietly commit notebooks with outputs
  embedded. One notebook per person, never a shared one.
- **Per-developer dbt targets** — `dbt run --target dev_<name>` — so nobody
  builds into someone else's dataset.
- **Clean at the earliest layer with enough context, and never twice** (§6).
  dlt does types only; staging does single-table structure; intermediate does
  cross-table logic; marts do shaping; tests verify and never repair;
  **notebooks do no cleaning at all**.
- **Non-engineers install nothing.** `BigQuery Data Viewer` + `BigQuery Job
  User` on their own Google account, and they query `olist_marts` in the web
  console (§15).

## Three findings that are wrong by default

Worth knowing before writing any query — each is a documented trap, not a bug:

1. **`customer_id` is a per-order surrogate.** Segment on `customer_unique_id`
   (`dim_customer.customer_key`) or you will conclude Olist has no repeat
   business. The real figure is ~96k customers, ~3% repeating (§5.2).
2. **Joining payments to order items inflates revenue.** Payments are at order
   grain, items at line grain. Keep the facts separate (§5.2).
3. **The dataset ends mid-October 2018.** The apparent revenue collapse at the
   end of the series is truncation, not a business event. Exclude or annotate
   the incomplete tail in every time-series chart (§9).
