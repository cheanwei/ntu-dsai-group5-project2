"""The pipeline: Kaggle -> GCS -> BigQuery olist_raw -> dbt -> marts.

    uv run python orchestration/run_all.py
    uv run python orchestration/run_all.py --dry-run
    uv run python orchestration/run_all.py --bucket-url gs://olist-raw-x/2026-08-29
    uv run python orchestration/run_all.py --ingest-date 2026-08-29

The single entrypoint, and the same one GitHub Actions runs nightly. It
materialises the whole graph in-process: nine raw tables from dlt, then the 21
dbt models built on top of them.

Design: architecture-design.md §8. GitHub Actions holds the scheduler role, so
no daemon is needed in production.

**The ephemeral-instance trap.** With `DAGSTER_HOME` unset, Dagster uses a
temporary directory *cleared on process exit*, and `materialize()` defaults to
an ephemeral instance — so run history does not survive. On a GitHub Actions
runner that is true either way, because the runner itself is destroyed. This is
designed for, not discovered:

- **The durable evidence is static HTML**: the `dbt docs` site, uploaded as an
  artifact with `if: always()` and deployed to GitHub Pages.
  Permanent URLs, citable from the report and the deck.
- **Run history is local.** Set `DAGSTER_HOME` and Dagster keeps history as
  SQLite under it (§8, `dagster.yaml`) — enough for `dagster dev` on a laptop
  and the console in `orchestration/deploy/`. Shared history across CI and
  every laptop would need a hosted database; the project deliberately has
  none.

Say so in the report: run history is ephemeral in CI by design because the
orchestrator is not hosted. A documented limitation reads as engineering
judgement; the same limitation discovered in Q&A does not.

Owner: lane A1.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Generator
from contextlib import contextmanager
from datetime import date
from pathlib import Path

from dagster import DagsterInstance, materialize
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent

# Running this as a path — `python orchestration/run_all.py`, which is what the
# workflow does — puts orchestration/ on sys.path rather than the repo root, so
# `import orchestration` would fail with what looks like a broken install. The
# `-m` form does not need this; the line is here because the CI step, the
# README and the CI step all invoke a file by path.
sys.path.insert(0, str(REPO_ROOT))

# **`override=False` is the whole safety argument.** Anything already exported
# wins, so this cannot change a run where the environment is authoritative: not
# in GitHub Actions, which injects secrets as variables and has no .env on the
# runner, and not on the VM, where Compose's `env_file:` has already injected
# the same keys. It only fills gaps, and the gap it fills is a laptop.
#
# Without it this and `dagster dev` disagree over the same graph: the latter
# loads .env from the working directory itself
# (`_inject_local_env_file` in dagster/_cli/utils.py) while running this file by
# path loads nothing. Same asset graph, and only one of them needs a sourced
# shell — which reads as the pipeline being broken rather than the shell being
# empty. Dagster's own version *overrides* the environment; this one does not,
# because a variable you exported deliberately should beat a file you edited
# last week.
#
# This is narrower than "modules load .env": nothing in ingestion/ or
# orchestration/ does, so a *library* import still cannot pick up ambient
# configuration. Only the entrypoint does, where the shell is the user.
ENV_FILE = REPO_ROOT / ".env"
if ENV_FILE.exists():
    load_dotenv(ENV_FILE, override=False)

from ingestion.config import config as ingestion_config  # noqa: E402
from orchestration.assets import (  # noqa: E402
    RAW_ZONE_URI_ENV,
    ingestion_assets,
    olist_raw_tables,
    transform_assets,
)
from orchestration.definitions import all_assets  # noqa: E402
from orchestration.resources import (  # noqa: E402
    DBT_TARGET_ENV,
    DEFAULT_DBT_TARGET,
    RAW_BUCKET_ENV,
    build_resources,
)


@contextmanager
def build_instance() -> Generator[DagsterInstance]:
    """The instance this run records into.

    `DagsterInstance.get()` *raises* when `DAGSTER_HOME` is unset rather than
    falling back, so the fallback is made here. On a runner destroyed after the
    job the history is worthless either way (see the module docstring), and
    refusing to start over storage nobody will read would be the wrong failure.
    """
    if os.environ.get("DAGSTER_HOME"):
        yield DagsterInstance.get()
    else:
        print("DAGSTER_HOME is not set: running with an ephemeral instance, no run history.")
        with DagsterInstance.ephemeral() as instance:
            yield instance


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python orchestration/run_all.py",
        description="Materialise the Olist asset graph: Kaggle -> olist_raw -> marts.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would run — resolved config and asset selection — and exit.",
    )
    parser.add_argument(
        "--bucket-url",
        help="Load from an existing raw-zone prefix (gs://bucket/YYYY-MM-DD), "
        "skipping the Kaggle download and the upload.",
    )
    parser.add_argument(
        "--ingest-date",
        help="Raw-zone prefix to fill, YYYY-MM-DD. Defaults to today. Re-using a "
        "date overwrites that prefix in place.",
    )
    parser.add_argument(
        "--bucket",
        help=f"Raw-zone bucket. Defaults to ${RAW_BUCKET_ENV}.",
    )
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Stop at olist_raw and build no models. For working on ingestion "
        "while the dbt models are incomplete.",
    )
    return parser


def plan(args: argparse.Namespace) -> str:
    """What a run with these arguments would do. Resolves, loads nothing."""
    cfg = ingestion_config()
    ingest_date = args.ingest_date or date.today().isoformat()
    bucket = args.bucket or os.environ.get(RAW_BUCKET_ENV) or f"<unset ${RAW_BUCKET_ENV}>"
    target = args.bucket_url or f"gs://{bucket}/{ingest_date}"

    lines = [
        f"kaggle     {cfg.kaggle_dataset}",
        f"raw zone   {target}" + ("   (existing — download skipped)" if args.bucket_url else ""),
        f"dataset    {cfg.dataset} ({cfg.location})",
        f"contract   {cfg.schema_contract}",
        "dbt target " + (
            "skipped (--skip-dbt)"
            if args.skip_dbt
            else os.environ.get(DBT_TARGET_ENV, DEFAULT_DBT_TARGET)
        ),
        f"assets     {num_assets(selection(args))} of {num_assets(all_assets)}",
        f"tables     {len(cfg.tables)}",
    ]
    lines += [f"           {t.file} -> {t.table}" for t in cfg.tables]
    return "\n".join(lines)


def num_assets(defs: list) -> int:
    """Asset keys, not definitions. Two of the four definitions are multi-assets
    — nine dlt tables and 21 dbt models — so counting definitions would report
    4 where the graph has 32."""
    return sum(len(d.keys) for d in defs)


def selection(args: argparse.Namespace) -> list:
    """Which assets this run materialises.

    Two flags cut the graph, at opposite ends and independently. `--bucket-url`
    says the raw zone is already filled, so the run starts at the loader rather
    than the download; `--skip-dbt` stops it at `olist_raw`.

    **`--skip-dbt` is a stopgap.** The marts models are `select *` stubs
    (`TODO(A2)`), so a full run fails in dbt however healthy the load was —
    which makes the exit code useless as a signal for anyone working on
    ingestion. Delete the flag once those models are written; it exists to
    separate "my change broke the load" from "the models are not built yet".
    """
    stages = [olist_raw_tables] if args.bucket_url else list(ingestion_assets())
    if not args.skip_dbt:
        stages += transform_assets()
    return stages


def run_config(args: argparse.Namespace) -> dict:
    """Config for the assets that take any. Empty unless a flag asked for it."""
    if args.ingest_date and not args.bucket_url:
        return {"ops": {"gcs_raw_files": {"config": {"ingest_date": args.ingest_date}}}}
    return {}


def main(argv: list[str] | None = None) -> int:
    """Materialise the graph and return a process exit code."""
    args = build_parser().parse_args(argv)

    if args.dry_run:
        print(plan(args))
        return 0

    # Both are read by resources that resolve at run time, so setting them here
    # is what makes a flag reach an asset without threading config through the
    # whole graph. Neither overwrites a value the caller set deliberately on the
    # command line, because the command line is what set them.
    if args.bucket:
        os.environ[RAW_BUCKET_ENV] = args.bucket
    if args.bucket_url:
        os.environ[RAW_ZONE_URI_ENV] = args.bucket_url

    with build_instance() as instance:
        result = materialize(
            selection(args),
            resources=build_resources(),
            instance=instance,
            run_config=run_config(args),
            # Return an exit code rather than a traceback: the workflow's later
            # steps run `if: always()` and publish the reports either way (§8).
            raise_on_error=False,
        )
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
