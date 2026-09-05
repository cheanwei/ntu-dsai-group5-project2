# scripts/

Things a developer runs by hand. Nothing here is imported by `ingestion/`,
`transform/` or `orchestration/`, and nothing here runs in CI — the scheduled
pipeline is Dagster assets (§8).

`bootstrap_env.py` exists because six people each have to perform the same local
setup correctly (§15). `provision_gcp.sh` exists because one person has to
perform the cloud half exactly once, with rights the other five do not have.

**Running the pipeline is not here.** That is `orchestration/run_all.py`, the
one entrypoint a laptop and CI both use. `run_ingestion.py` below is a fallback
kept deliberately out of the top-level README so there is only ever one
documented command.

## `bootstrap_env.py`

Fills the per-developer values in `.env`.

```bash
uv run python scripts/bootstrap_env.py --token-file ~/tok.txt
uv run python scripts/bootstrap_env.py                      # prompts, needs a terminal
uv run python scripts/bootstrap_env.py ~/Downloads/kaggle.json   # legacy
uv run python scripts/bootstrap_env.py --force              # overwrite existing
```

The token comes from `--token-file`, from redirected stdin
(`... < ~/tok.txt`), or from a hidden prompt. The prompt needs a real terminal —
under a wrapper shell or an IDE console `sys.stdin` is not a TTY, so use
`--token-file` there.

**Do not pipe it from `echo`.** `echo '<token>' | ...` still works, but it
writes the credential into your shell history, which is the leak the safety
properties below exist to prevent. A file you can delete does not.

### Creating a token from the terminal

**This is the default.** With no token supplied and a real terminal available,
the script mints one — you do not need a flag:

```bash
uv run python scripts/bootstrap_env.py
uv run python scripts/bootstrap_env.py --expiration 7d
uv run python scripts/bootstrap_env.py --no-oauth   # paste one instead
```

It runs `kaggle auth login`, which prints a URL you approve in a browser and
then asks for a verification code, then mints the token through the SDK.

Precedence, highest first: `--token-file`, redirected stdin, minting, paste prompt.
A token you supply always wins — minting only happens when nothing was given.

Two constraints:

- **It needs a real terminal.** The verification-code step reads from stdin, so
  run it directly — under a wrapper shell or an IDE console it refuses up front
  rather than dying on the read.
- **These tokens expire.** The default here is **30 days**
  (`DEFAULT_EXPIRATION`), overriding `kagglesdk`'s own 12-hour
  `DEFAULT_ACCESS_TOKEN_EXPIRATION` so a token lasts the length of the project.
  Kaggle may clamp the request server-side. For CI still prefer a durable token
  from `kaggle.com/settings` as the `KAGGLE_API_TOKEN` secret — a token that
  lapses mid-project is a nightly run that fails at 08:00 (§8).

### Why this does not use `kaggle auth print-access-token --expiration`

That flag is broken in Kaggle CLI 2.2.4. `kaggle_api_extended.py:1641` reads:

```python
delta = relativedelta(**{duration_str[-1]: int(duration_str[:-1])})
```

It passes the suffix *letter* as the keyword, so `30d` becomes `d=30` and
`relativedelta` raises `TypeError` — which the surrounding `except ValueError`
does not catch. Every value fails, including the `6h` in Kaggle's own
documentation.

The SDK beneath it takes a plain `timedelta`, so `bootstrap_env.py` parses the
duration itself and calls `KaggleCredentials.generate_access_token()` directly.
If that internal path ever breaks it warns and falls back to the CLI's default
12-hour token rather than failing outright.

There is still no way to mint the *first* credential with no browser at all:
authorising a token requires already being authenticated.

The `kaggle` CLI this needs is in the **dev** dependency group only. The
pipeline authenticates with `KAGGLE_API_TOKEN` and never needs it, so it stays
out of the CI install footprint (§3).

### Which credential

Kaggle now issues an **API token** string, not a `kaggle.json` download. That is
also what kagglehub prefers — `kagglehub/config.py` checks `KAGGLE_API_TOKEN`
first and labels the username/key branch below it "Legacy credentials support".
Its full order:

| Priority | Source |
|---|---|
| 1 | `KAGGLE_API_TOKEN` (a value, or a path to a file holding one) |
| 2 | `~/.kaggle/access_token` |
| 3 | `KAGGLE_USERNAME` + `KAGGLE_KEY` — legacy |
| 4 | `~/.kaggle/kaggle.json` — legacy |

The script writes `KAGGLE_API_TOKEN`. If you point it at an old `kaggle.json`
it writes the legacy pair instead, so teammates with existing credentials are
not forced to re-issue.

### Safety properties, all tested

- **The token is never a command-line argument.** It is prompted for without
  echo, or read from a file — a credential in shell history is a credential
  leak.
- **Refuses to write if `.env` is not gitignored.** Writing a credential into a
  tracked file is the §12 risk this convention exists to prevent.
- **Idempotent.** Values already set are kept, not clobbered, unless `--force`.
  Re-running cannot break a working `.env`.
- **Never prints a secret.** It reports `set` / `kept`, not the value.

With no credential available from any of the four sources there is nothing to
do, and it exits 1.

## `provision_gcp.sh`

Creates the data plane: the raw-zone bucket, `olist_raw` and `olist_marts`, and
the bucket IAM binding the pipeline needs.

```bash
./scripts/provision_gcp.sh --dry-run          # print every call, change nothing
./scripts/provision_gcp.sh
./scripts/provision_gcp.sh --grant-bigquery   # also grant the SA on BigQuery
ENV_FILE=other.env ./scripts/provision_gcp.sh # read a different file
```

It reads `.env` at the repo root itself, so there is nothing to `source` first.
Two details there. It **reads** rather than sources: `source` executes the
file, which would run a Kaggle token containing a backtick or `$( )` as code
and export every unrelated variable besides, so it lifts out only the keys it
uses. And anything already exported wins, so `GCP_PROJECT=other
./scripts/provision_gcp.sh` still overrides and a pre-sourced `.env` changes
nothing.

This does not contradict "nothing loads `.env` for you" in the top-level
README. That rule is about `ingestion/` and `orchestration/`, where an ambient
`.env` would make a pipeline run depend on which shell started it. This is a
setup script run by hand, and `bootstrap_env.py` beside it already reads and
writes the same file.

Idempotent — every step checks for its resource first, so re-running after a
partial failure resumes rather than errors. `--dry-run` is total: every mutation
goes through one `run()` helper, so there is no path that changes state without
printing first.

### Why this is not part of `bootstrap_env.py`

Different cardinality and different rights. `bootstrap_env.py` runs once per
developer per clone, writes one gitignored local file, and needs no cloud
credentials at all. This runs once per project, mutates shared cloud state, and
needs project-admin. Folding it in would mean five of six teammates running the
documented setup command and getting a 403 on `storage.buckets.create` — from a
script whose contract is that it cannot fail on credentials you do not have.

It is a shell script for the same reason `orchestration/deploy/provision_vm.sh`
is: it is entirely `gcloud` and `bq` calls, and wrapping those in Python buys
argument plumbing and nothing else. The two scripts share their helper shapes
(`run`, `run_quiet`, `exists`, `--dry-run`) deliberately.

### `gcloud auth login`, not `application-default login`

ADC (`~/.config/gcloud/application_default_credentials.json`) is read by client
libraries through `google-auth`. `gcloud` and `bq` do not read it — they use
gcloud's own credential store. Running only the ADC command leaves the preflight
check failing with an empty account list.

ADC is unused everywhere in this project, not just here: `ingestion/` builds
clients from `GOOGLE_APPLICATION_CREDENTIALS`, which `google-auth` resolves
ahead of the ADC file.

### Location is checked, not just set

Bucket and dataset locations are immutable (§14), and the failure they cause is
misattributed: a non-US bucket fails the load into a US dataset with an error
that looks like a bucket permissions problem. So where a resource already
exists the script compares its location against `GCP_LOCATION` and stops on a
mismatch, rather than creating the rest and leaving you to find out at load
time.

### What it deliberately does not do

- **No dbt dataset.** dbt-bigquery creates its own target dataset when the
  account holds `bigquery.datasets.create`, using the `location` from
  `transform/profiles.yml`. Creating one here would be a second source of truth
  for a name only that file uses.
- **No BigQuery IAM by default.** The service account usually already holds
  those roles. `--grant-bigquery` is for a freshly minted one, and it is opt-in
  because unlike the bucket grant it is project-scoped: `jobUser` has no lower
  scope, and dataset-level `dataEditor` means patching each dataset's access
  list rather than adding one binding.
- **No service-account creation, and no key.** Keys are the §10 risk the rest
  of this directory is built around; the script reads the existing account's
  address out of the key file you already have and never mints another.

## `run_ingestion.py`

**A fallback, not the entrypoint.** `orchestration/run_all.py` does everything
this does and the dbt models besides, with the same four flags. Use this one
only when you want the load with Dagster out of the way:

- debugging `ingestion/` itself, where an asset-graph traceback is noise
- a machine where the graph will not import — a broken dbt manifest, say, which
  `run_all.py` needs at import time and this file does not

```bash
uv run python -m scripts.run_ingestion --dry-run    # resolve everything, load nothing
uv run python -m scripts.run_ingestion              # download, upload, load
uv run python -m scripts.run_ingestion --bucket-url gs://<bucket>/2026-08-29
```

`--bucket` defaults to `$GCP_RAW_BUCKET` and `--ingest-date` to today. It loads
`.env` itself when the file exists, never over the top of the environment, so
the chain is `--bucket` > exported variable > `.env`. Exits 2 with the variable
named if none of the three resolves.

**Why it is not in `ingestion/`.** It fuses what §8 models as several assets —
`kaggle_dataset`, `gcs_raw_files`, then the nine dlt assets — into one call.
That is right for a terminal and wrong for Dagster, where it would collapse the
lineage graph into a single opaque node. `orchestration/` therefore imports the
pieces from `ingestion/` directly and never imports this file:

| Asset | Comes from |
|---|---|
| `kaggle_dataset` | `ingestion.kaggle_to_gcs.download_dataset` |
| `gcs_raw_files` | `ingestion.kaggle_to_gcs.upload_to_gcs` |
| the nine dlt assets | `ingestion.gcs_to_bigquery.run_pipeline` over `ingestion.olist_source` |

What loads, and how, is `ingestion/config.yml` — not this script.
