"""CI entrypoint: materialise the whole graph in-process.

Design: architecture-design.md §8. GitHub Actions holds the scheduler role, so
no daemon is needed in production.

**The ephemeral-instance trap.** With `DAGSTER_HOME` unset, Dagster uses a
temporary directory *cleared on process exit*, and `materialize()` defaults to
an ephemeral instance — so run history does not survive. On a GitHub Actions
runner that is true either way, because the runner itself is destroyed. This is
designed for, not discovered:

- **The durable evidence is static HTML**: GX Data Docs and `dbt docs`,
  uploaded as artifacts with `if: always()` and deployed to GitHub Pages.
  Permanent URLs, citable from the report and the deck.
- **Run history is local.** Set `DAGSTER_HOME` and Dagster keeps history as
  SQLite under it (§8, `dagster.yaml`) — enough for `dagster dev` on a laptop
  and the console in `deploy/`. Shared history across CI and every laptop would
  need a hosted database; the project deliberately has none.

Say so in the report: run history is ephemeral in CI by design because the
orchestrator is not hosted. A documented limitation reads as engineering
judgement; the same limitation discovered in Q&A does not.

Owner: lane A1.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from dagster import DagsterInstance, materialize

# Running this as a path — `python orchestration/run_all.py`, which is what the
# workflow does — puts orchestration/ on sys.path rather than the repo root, so
# `import orchestration` would fail with what looks like a broken install. The
# `-m` form does not need this; the line is here because the CI step, the
# README and `scripts/run_ingestion.py` all invoke a file by path.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestration.definitions import all_assets  # noqa: E402
from orchestration.resources import build_resources  # noqa: E402


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


def main() -> int:
    """Materialise every asset and return a process exit code."""
    with build_instance() as instance:
        result = materialize(
            all_assets,
            resources=build_resources(),
            instance=instance,
            # Return an exit code rather than a traceback: the workflow's later
            # steps run `if: always()` and publish the reports either way (§8).
            raise_on_error=False,
        )
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
