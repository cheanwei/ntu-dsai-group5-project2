"""Dagster resources: the external systems the assets talk to.

Design: architecture-design.md §8. Configuration comes from the environment —
never from a committed file. In CI the same variables are GitHub Actions
secrets (§10).

Three of the four resources are thin wrappers over `ingestion/`, and the
thinness is the point. The pipeline logic stays in `ingestion/`, where it is
tested without Dagster; what a resource adds is a **seam**. `EnvVar` resolves
at run time rather than at load, so `dagster dev` opens on a laptop with no
`.env` sourced, and a test supplies a stand-in for Kaggle, the bucket and the
warehouse without patching module attributes.

Owner: lane A1.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

from dagster import ConfigurableResource, EnvVar
from dagster_dbt import DbtCliResource, DbtProject
from dagster_dlt import DagsterDltResource

from ingestion.gcs_to_bigquery import build_pipeline
from ingestion.kaggle_to_gcs import download_dataset, upload_to_gcs

REPO_ROOT = Path(__file__).parent.parent

# The dbt project directory. `@dbt_assets` reads the manifest emitted here, so
# there is no second DAG definition to drift out of sync (§3).
DBT_PROJECT_DIR = REPO_ROOT / "transform"
DBT_PROFILES_DIR = DBT_PROJECT_DIR

# Which profiles.yml target dbt builds into. `dev` writes to dbt_dev, so a
# laptop cannot touch the shared marts by forgetting a flag; the nightly run
# sets DBT_TARGET=prod, whose dataset `olist` plus dbt_project.yml's
# `+schema: marts` is what makes the analyst-facing `olist_marts` (§5.1, §15).
DBT_TARGET_ENV = "DBT_TARGET"
DEFAULT_DBT_TARGET = "dev"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR)


def prepare_dbt_manifest() -> Path:
    """The manifest `@dbt_assets` reads, generated if it is not already there.

    `transform/target/` is gitignored, so on a fresh clone and on a CI runner
    the manifest does not exist and importing the asset graph would fail at
    decoration time — before any error message about dbt could be printed.

    Two mechanisms, because they cover different situations. `prepare_if_dev()`
    re-parses on every `dagster dev` launch, which is what keeps a long-lived
    dev process from serving a manifest that predates the model you just
    edited. It deliberately does nothing outside that CLI, so the parse below
    covers the other two entrypoints: `run_all.py` on a laptop and the same
    file in GitHub Actions.

    `dbt parse` resolves `env_var()` in profiles.yml, so the environment has to
    be loaded by now — it is, because run_all.py loads .env before importing
    anything from orchestration/, and the dagster CLI injects it before loading
    the code location.
    """
    dbt_project.prepare_if_dev()
    if manifest_is_stale(DBT_PROJECT_DIR, dbt_project.manifest_path):
        # The packages supply tests the models use — dbt_utils' two, and the
        # dbt_expectations assertions of §7 — and parse fails on an
        # unresolvable macro without them. transform/dbt_packages/ is
        # gitignored, so a fresh clone has to install them before anything can
        # read the project: doing it here is what keeps
        # `uv run python orchestration/run_all.py` working as the first command
        # after `uv sync`.
        if deps_are_missing(DBT_PROJECT_DIR):
            subprocess.run(_dbt("deps"), check=True, env=_parse_env())
        subprocess.run(_dbt("parse"), check=True, env=_parse_env())
    return dbt_project.manifest_path


# The paths `dbt parse` compiles a manifest from. What is *not* here matters as
# much: `target/` is dbt's own output — it writes run_results.json and the
# whole compiled/ tree after manifest.json in the same invocation, so comparing
# against it would call every manifest stale the instant it was created — and
# `dbt_packages/` is vendored code that moves only when package-lock.yml does,
# which is `deps_are_missing`'s question rather than this one.
DBT_SOURCE_DIRS = ("models", "macros", "seeds", "tests", "snapshots", "analyses")
DBT_SOURCE_FILES = ("dbt_project.yml", "packages.yml", "package-lock.yml", "profiles.yml")


def manifest_is_stale(project_dir: Path, manifest_path: Path) -> bool:
    """Whether the manifest is older than the project it claims to describe.

    **This is what keeps the dbt tests wired.** `@dbt_assets` derives one asset
    check per dbt test from the manifest, once, when the code location loads;
    `dbt build` at run time reads the project files instead. A manifest that
    predates a schema.yml splits the two — dbt executes tests that Dagster has
    no check key to record, so they run in BigQuery and land nowhere. That is
    the state a checkout is in after pulling a commit that adds tests, which is
    how the ten dbt_expectations assertions arrived.

    Only the absence of a manifest used to trigger a parse, which covered the
    fresh clone and the CI runner and nothing else. `prepare_if_dev()` covers a
    third case — it re-parses on every `dagster dev` launch — and the two
    remaining entrypoints, `run_all.py` and the container in
    `orchestration/deploy/`, are exactly the ones where nobody is watching the
    UI to notice checks had gone missing.

    mtime rather than a content hash: a hash means reading every model on every
    load to detect something that changes a few times a week, and a spurious
    re-parse costs seconds while a missed one costs a silently unguarded build.
    """
    if not manifest_path.exists():
        return True
    manifest_mtime = manifest_path.stat().st_mtime
    return any(path.stat().st_mtime > manifest_mtime for path in _dbt_source_files(project_dir))


def _dbt_source_files(project_dir: Path) -> Iterator[Path]:
    """Every file a parse reads, under the paths declared in dbt_project.yml."""
    for name in DBT_SOURCE_FILES:
        path = project_dir / name
        if path.is_file():
            yield path
    for name in DBT_SOURCE_DIRS:
        directory = project_dir / name
        if directory.is_dir():
            yield from (path for path in directory.rglob("*") if path.is_file())


def deps_are_missing(project_dir: Path) -> bool:
    """Whether `dbt deps` has to run before the project can be parsed.

    `DbtProject.has_uninstalled_deps` is not enough, and it is worth saying why
    rather than leaving the reimplementation looking like duplication: it asks
    only whether `dbt_packages/` *exists* (dbt_project.py:318). So it answers
    False for the case this project just hit — a commit adds a package to
    packages.yml, the install directory is still there holding yesterday's set,
    and the parse fails on a macro from the package nobody installed.

    Comparing package-lock.yml against the install directory answers the real
    question. A `git pull` that updates the lock leaves it newer; a `dbt deps`
    that installs leaves the directory newer.

    The comparison has to stay this conservative because `dbt deps` reaches the
    package index, and this module's docstring promises `uv run pytest` runs
    with no credentials and no network. Re-installing on every load would break
    that for a directory that is already correct.
    """
    if not (project_dir / "packages.yml").is_file():
        return False

    install_dir = project_dir / "dbt_packages"
    if not install_dir.is_dir():
        return True

    lock = project_dir / "package-lock.yml"
    return lock.is_file() and lock.stat().st_mtime > install_dir.stat().st_mtime


def _dbt(command: str) -> list[str]:
    """A dbt invocation, resolved next to this interpreter so it is the venv's
    dbt rather than whatever `dbt` happens to be first on PATH.

    `python -m dbt.cli.main` would do the same job but warns about re-importing
    an already-imported package, and a warning nobody can act on is noise on
    every first run.
    """
    executable = Path(sys.executable).with_name("dbt")
    argv = [str(executable)] if executable.exists() else [sys.executable, "-m", "dbt.cli.main"]
    return [
        *argv, command, "--quiet",
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROFILES_DIR),
    ]


def _parse_env() -> dict[str, str]:
    """Environment for `dbt parse`, with placeholders for anything unset.

    `dbt parse` resolves every `env_var()` in profiles.yml and fails on a
    missing one — but it only reads the *shape* of the project and never opens
    a connection, so the values need to exist rather than to be right.

    Without this, importing the asset graph would need real credentials, and
    two things this project promises would stop being true: `uv run pytest`
    runs with no credentials and no network, and `dagster dev` opens on a
    laptop that never filled in .env (see definitions.py). A wrong value cannot
    leak into a run — this environment is scoped to the parse subprocess, and a
    real run resolves the same variables again through DbtCliResource.
    """
    env = dict(os.environ)
    for key, placeholder in (
        ("GCP_PROJECT", "unset-at-parse-time"),
        ("GOOGLE_APPLICATION_CREDENTIALS", str(REPO_ROOT / "unset-at-parse-time.json")),
    ):
        env.setdefault(key, placeholder)
    return env

# Where the Kaggle download lands before it is uploaded. Local scratch, not a
# durable artifact — the durable copy is the one in GCS, which is the whole
# point of the raw zone (§4). Under `data/`, which .gitignore already excludes
# along with every CSV, so the 126 MB cannot be committed by accident.
STAGING_DIR = REPO_ROOT / "data" / "staging"

# Named once. `run_all.py --bucket` writes this variable rather than threading a
# value through the graph, because `RawZone` resolves it at run time.
RAW_BUCKET_ENV = "GCP_RAW_BUCKET"


def gcp_project() -> str:
    return os.environ["GCP_PROJECT"]


class KaggleDataset(ConfigurableResource):
    """The Kaggle source dataset, downloaded at the pinned version (§4).

    `staging_dir` is configuration rather than a temp directory because the
    download and the upload are two assets: the second has to find what the
    first produced, and a `TemporaryDirectory` would be gone by then. It also
    makes re-uploading a download that already cost 126 MB free.
    """

    staging_dir: str

    def download(self) -> str:
        """Download and unzip into `staging_dir`. Returns that directory."""
        return download_dataset(self.staging_dir)


class RawZone(ConfigurableResource):
    """The GCS raw zone.

    The bucket must be US multi-region — one outside the US fails the load into
    a US dataset with an error that appears to blame the bucket (§14).
    """

    bucket: str

    def upload(self, local_dir: str, ingest_date: str) -> str:
        """Upload the CSVs to `gs://<bucket>/<ingest_date>/`, returning that URI."""
        return upload_to_gcs(local_dir, self.bucket, ingest_date)


class Warehouse(ConfigurableResource):
    """BigQuery `olist_raw`, reached through dlt.

    The pipeline is built per run, not once at import, so `BIGQUERY_RAW_DATASET`
    is read when the run starts rather than frozen into the code location — a
    long-lived `dagster dev` process would otherwise serve a stale dataset.
    """

    def pipeline(self):
        return build_pipeline()

    def load_kwargs(self) -> dict:
        """Extra keyword arguments for `pipeline.run`. None in production; a
        test overrides this to pick a file format its destination supports."""
        return {}


def build_resources() -> dict:
    """Resource dict passed to `Definitions`.

    `DbtCliResource` and the GX context join this dict with the assets that
    need it — `dbt_models` in `assets.py`.
    """
    return {
        "kaggle": KaggleDataset(staging_dir=str(STAGING_DIR)),
        "raw_zone": RawZone(bucket=EnvVar(RAW_BUCKET_ENV)),
        "warehouse": Warehouse(),
        "dlt": DagsterDltResource(),
        # Target resolved here rather than on `dbt_project` above, so it is read
        # when a run starts and not frozen into the code location at import.
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            target=os.environ.get(DBT_TARGET_ENV, DEFAULT_DBT_TARGET),
        ),
    }
