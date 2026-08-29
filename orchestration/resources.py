"""Dagster resources: the external systems the assets talk to.

Design: architecture-design.md §8. Configuration comes from the environment —
never from a committed file. In CI the same variables are GitHub Actions
secrets (§10).

Owner: lane A1.
"""

from __future__ import annotations

import os
from pathlib import Path

# The dbt project directory. `@dbt_assets` reads the manifest emitted here, so
# there is no second DAG definition to drift out of sync (§3).
DBT_PROJECT_DIR = Path(__file__).parent.parent / "transform"
DBT_PROFILES_DIR = DBT_PROJECT_DIR

# Great Expectations context (§7).
GX_PROJECT_DIR = Path(__file__).parent.parent / "quality" / "great_expectations"


def gcp_project() -> str:
    return os.environ["GCP_PROJECT"]


def raw_bucket() -> str:
    """GCS raw-zone bucket. Must be US multi-region — a bucket outside the US
    fails the load into a US dataset with an error that appears to blame the
    bucket (§14)."""
    return os.environ["GCP_RAW_BUCKET"]


def build_resources() -> dict:
    """Resource dict passed to `Definitions`.

    TODO(A1): DbtCliResource, DagsterDltResource, a GCS client, and the GX
    context.
    """
    raise NotImplementedError("TODO(A1): assemble DbtCliResource + DagsterDltResource")
