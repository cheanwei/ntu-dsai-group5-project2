# Olist Data Platform

End-to-end data platform over the [Olist Brazilian E-Commerce
dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce): nine
CSVs, ~120 MB, ~100k orders from September 2016 to October 2018.

```
Kaggle CSVs → GCS raw zone → dlt → BigQuery → dbt (staging → intermediate → marts)
                                       ↓                        ↓
                              dbt + dbt-expectations    SQLAlchemy + pandas
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
| Pipeline runs | GitHub Actions tab |

## Setup

Prerequisites: Python 3.11+, [uv](https://docs.astral.sh/uv/), a GCP project
with BigQuery enabled, and a Kaggle API token.

```bash
git clone <repo> && cd <repo>
uv sync                          # add --extra duckdb for the §12 fallback target
uv run nbstripout --install      # EVERY person, EVERY clone — see below

# Creates .env from .env.example, then mints a 30-day Kaggle token into it.
# Needs a real terminal for the browser approval step:
uv run python scripts/bootstrap_env.py
```

Then fill in `GCP_PROJECT`, `GCP_RAW_BUCKET` and
`GOOGLE_APPLICATION_CREDENTIALS` in `.env`. dbt packages and its manifest are
installed on first run, so there is no `dbt deps` step.

### The cloud side, once per project

The bucket, the datasets and the one IAM binding the pipeline needs. Run by
whoever owns the GCP project, not by every teammate — it needs project-admin
rights the rest of the team does not have.

```bash
gcloud auth login                        # NOT `application-default login` — see below
./scripts/provision_gcp.sh --dry-run     # print every call, change nothing
./scripts/provision_gcp.sh
```

Fill in `GCP_PROJECT` and `GCP_RAW_BUCKET` in `.env` first — this script reads
that file itself, so there is nothing to `source`. Idempotent, so re-running
after a partial failure resumes. Three things it encodes that are easy to get
wrong by hand:

- **Everything is US multi-region and that is immutable** (§14). A bucket
  outside the US fails the load into a US dataset with an error that appears to
  blame the bucket. Where a resource already exists the script checks its
  location and stops rather than letting you discover this at load time.
- **The service account needs the bucket as well as BigQuery.** A key minted
  for BigQuery has no storage role, and the failure comes late — the Kaggle
  download succeeds, then the first upload returns a 403 naming
  `storage.objects.create`. The grant goes on the bucket, not the project, so
  the account stays scoped to the raw zone, and it is `objectUser` rather than
  `objectCreator`: the upload needs create, and dlt needs get and list to read
  those same CSVs back during the load. The account is read from the
  `client_email` in your `GOOGLE_APPLICATION_CREDENTIALS` key file. Pass
  `--grant-bigquery` as well if it is newly minted and holds no BigQuery roles.
- **`gcloud auth login`, not `gcloud auth application-default login`.** ADC is
  a separate credential file that only client libraries read; `gcloud` and `bq`
  use their own store, so the ADC command alone leaves both unauthenticated.
  Nothing in this project uses ADC — the pipeline authenticates with the
  service-account key, which `google-auth` resolves ahead of it.

There is no dbt dataset to create: dbt-bigquery makes its own target dataset on
first build, in the `location` from `transform/profiles.yml`.

**The service-account key never enters git.** `.gitignore` excludes `*.json`
from the first commit. In CI the key is the `GCP_SA_KEY` secret, written to
disk by the workflow and deleted in the same job.

## Running it

One command, from a filled-in `.env` to a loaded raw zone:

```bash
uv run python orchestration/run_all.py --skip-dbt
```

11 assets in one process: the Kaggle download, the nine CSVs in the raw zone,
and the nine `olist_raw` tables dlt loads. Budget about five minutes cold,
nearly all of it the 126 MB Kaggle download; a re-run is much faster.

**Drop `--skip-dbt` to build the marts too** — the other 21 assets, with every
dbt test arriving as an asset check on the model it guards. That is the real
command and what the deployed daemon runs daily at 08:00 SGT, but **it fails
today**: the marts models are still `select *` stubs (`TODO(A2)` in
`transform/models/`), so the run ends `PASS=54 ERROR=6`, every error a stub
referencing a column that does not exist yet. The flag is here so that failure
does not look like a broken load. Delete it from this command when those models
land.

**Nothing to `source` first** — the entrypoint reads `.env` itself. A variable
you export still wins over the file.

Confirm what landed:

```bash
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM \`${GCP_PROJECT}.olist_raw.olist_orders_dataset\`"
```

About 99k orders. Zero rows or a missing table means the load did not reach
BigQuery — read the Dagster output rather than re-running blind.

### Options

```bash
--dry-run                              # resolve config and selection, run nothing
--bucket-url gs://<bucket>/2026-08-29  # raw zone already filled: skip the download
--ingest-date 2026-08-29               # which raw-zone prefix to fill (default: today)
--bucket <name>                        # override $GCP_RAW_BUCKET
--skip-dbt                             # stop at olist_raw (in the command above)
```

`--bucket-url` is the iteration loop and the recovery path: the download and
upload are the slow half, so when they have already succeeded, point at the
prefix and re-run only the load and the models. The two cuts are independent
and compose — both together materialise the nine `olist_raw` tables and nothing
else.

**`--skip-dbt` is temporary** — see above. It is in the documented command only
because the dbt layer is unwritten; it is not a design choice, and the pipeline
is not finished while it is needed.

The ingest date *is* the raw-zone prefix. Re-using one overwrites it in place,
a new one lands beside it, and the load is `replace` either way — so re-running
never duplicates rows.

### Iterating on one layer

```bash
uv run dagster dev -m orchestration.definitions   # UI on :3000, materialise any subset
cd transform && uv run dbt build                  # models only, against dbt_dev
uv run dbt docs generate && uv run dbt docs serve
```

`dagster dev` is also the best view of the graph — the lineage screenshot for
the Technical Overview slide comes from here. Two things to know: it loads
`.env` *over* your shell rather than under it, so change the file rather than
exporting; and bare `dbt` commands read `GCP_PROJECT` and
`GOOGLE_APPLICATION_CREDENTIALS` through `env_var()` with no `.env` support of
their own, so those need `set -a; source .env; set +a`.

`DBT_TARGET` picks the profiles.yml target. It defaults to `dev`, which writes
to `dbt_dev`; the scheduled run sets `prod`, whose dataset plus
`+schema: marts` is the `olist_marts` analysts query.

### What loads, and how

**`ingestion/config.yml`** — the pinned Kaggle version, the nine tables, and
every one of their 52 columns with the type it must land as. The schema is
fixed, not inferred: `columns:` is exhaustive, so a column present in the CSV
but missing from the YAML is a *new* column at load time and the frozen
contract rejects it. Read it before changing anything in `ingestion/*.py`; most
changes belong in the YAML, and `--dry-run` prints exactly what it resolves to.

Two failures are expected behaviour, not bugs: an unexpected column fails the
load (the frozen schema contract, §4 — on the first run, not the second, since
the schema is declared rather than learned), and a missing source file fails the
download naming the file (the version pin no longer matching what Kaggle
serves). Both are meant to stop the run.

```bash
uv run pytest                    # 76 tests, no credentials or network needed
```

Both triggers are held by the Dagster daemon running in Docker Compose on the
GCP VM (`orchestration/deploy/`), not by a workflow cron:

| Name | Kind | Fires | Materialises |
| --- | --- | --- | --- |
| `daily_refresh` | schedule | 08:00 SGT | Kaggle → GCS → `olist_raw` |
| `automation_conditions` | sensor | when the load lands | the dbt layer, per model |

The split is deliberate. Nothing external announces a change to the Kaggle
dump, so ingestion is pulled on a cron; the load finishing *is* an event we
emit, so dbt is triggered by it rather than by a second cron
(`AutomationCondition.eager()` in `orchestration/assets.py`). An ingestion
failure therefore produces no dbt run at all, instead of rebuilding the marts
on yesterday's raw tables and reporting green.

To run it the way CI does without waiting, the **pipeline** workflow is
`workflow_dispatch` — trigger it from the Actions tab; that path also publishes
the dbt docs site to Pages.

### If the load fails with `CERTIFICATE_VERIFY_FAILED`

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

Fix the interpreter once; existing venvs inherit it, so there is nothing to
rebuild:

```bash
"/Applications/Python 3.11/Install Certificates.command"
```

Quoted, not backslash-escaped: the path has two spaces in it and a stray escape
sends the shell looking for `/Applications/Python`. Re-run the check above — it
should now print a path ending `etc/openssl/cert.pem`. Then resume without
re-downloading, since the raw zone is already populated:

```bash
uv run python orchestration/run_all.py --skip-dbt --bucket-url gs://<bucket>/<ingest-date>
```

## Repository layout

| Path | Contents | Lane |
|---|---|---|
| `ingestion/` | Kaggle → GCS → dlt → `olist_raw` (§4) | A1 |
| `scripts/` | Run by hand: `.env` bootstrap and one-time GCP provisioning | A1 |
| `transform/` | dbt project: 9 staging, 4 intermediate, 8 marts (§5) | A2 |
| `transform/tests/` | Tier-2 business invariants as dbt tests (§7) | B1 |
| `orchestration/` | Dagster assets, resources, schedule, and `run_all.py` (§8) | A1 |
| `orchestration/deploy/` | Compose deployment that runs the graph — stretch, week 3 only (§8) | A1 |
| `notebooks/` | Four analyses over the marts (§9) | B2 |
| `dashboards/` | Flask + Streamlit scaffold — stretch, not a deliverable (§11) | B2 |
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
- **Isolation is per project, not per dataset.** There is one `dev` target and
  it writes to `dbt_dev`, which dbt creates on first build. To keep your work
  off everyone else's, point `GCP_PROJECT` at your own project and run
  `scripts/provision_gcp.sh` there. That is the whole pipeline, not just dbt:
  sources resolve to `$GCP_PROJECT.olist_raw`, so a personal project needs its
  own raw zone — own bucket, own service-account key, own Kaggle download.
  Sharing a project means sharing `dbt_dev`, so coordinate before building.
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
