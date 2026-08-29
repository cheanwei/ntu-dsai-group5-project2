# scripts/

Things a developer runs by hand. Nothing here is imported by `ingestion/`,
`transform/` or `orchestration/`, and nothing here runs in CI — the scheduled
pipeline is Dagster assets (§8).

`bootstrap_env.py` exists because six people each have to perform the same local
setup correctly (§15). `run_ingestion.py` exists because the load is one command
for a human but several assets for Dagster, and those are different shapes.

## `bootstrap_env.py`

Fills the per-developer values in `.env`.

```bash
uv run python scripts/bootstrap_env.py --token-file ~/tok.txt
uv run python scripts/bootstrap_env.py --name cheanwei      # prompts, needs a terminal
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
uv run python scripts/bootstrap_env.py --name cheanwei
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
  lapses mid-project is a nightly run that fails at 02:00 (§8).

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

`--name` is independent of the credential: it sets `DBT_DEV_DATASET` even with
no token available, so you can do it before you have one. With neither a
credential nor `--name` there is nothing to do, and it exits 1.

## `run_ingestion.py`

Runs the ingestion pipeline end to end: Kaggle → GCS raw zone → BigQuery
`olist_raw`.

```bash
uv run python -m scripts.run_ingestion --dry-run    # resolve everything, load nothing
uv run python -m scripts.run_ingestion              # download, upload, load
uv run python -m scripts.run_ingestion --bucket-url gs://<bucket>/2026-08-29
```

`--bucket` defaults to `$GCP_RAW_BUCKET` and `--ingest-date` to today. Exits 2
with the variable named if the bucket cannot be resolved — usually a shell that
never sourced `.env`, since nothing in this project loads it implicitly.

**Why this is not in `ingestion/`.** It fuses what §8 models as several assets —
`kaggle_dataset`, `gcs_raw_files`, then the nine per-table dlt assets — into one
call. That is right for a terminal and wrong for Dagster, where it would
collapse the lineage graph into a single opaque node. `orchestration/` therefore
imports the pieces from `ingestion/` directly and never imports this file:

| Asset | Comes from |
|---|---|
| `kaggle_dataset` | `ingestion.kaggle_to_gcs.download_dataset` |
| `gcs_raw_files` | `ingestion.kaggle_to_gcs.upload_to_gcs` |
| the nine dlt assets | `ingestion.pipeline.run` over `ingestion.olist_source` |

What loads, and how, is `ingestion/config.yml` — not this script.
