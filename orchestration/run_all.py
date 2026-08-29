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

import sys


def main() -> int:
    """Materialise every asset and return a process exit code.

    TODO(A1):
        from dagster import DagsterInstance, materialize
        instance = DagsterInstance.get()   # local history if DAGSTER_HOME is set
        result = materialize([...], resources=..., instance=instance)
        return 0 if result.success else 1
    """
    raise NotImplementedError("TODO(A1): materialize() the asset graph")


if __name__ == "__main__":
    sys.exit(main())
