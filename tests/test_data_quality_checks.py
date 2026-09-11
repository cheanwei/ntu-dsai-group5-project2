"""The dbt tests, as Dagster sees them (§7).

Two concerns, and the second only holds because of the first.

**Wiring.** Every dbt test — the built-in ones, `dbt_utils`, and the
`dbt_expectations` tier-2 assertions — has to arrive as an *asset check* on the
asset it guards, which is what §7 asks for: a quality failure lands on the
model that produced it rather than on a nameless task. Nothing in
`orchestration/` declares those checks; they are derived from the dbt manifest
by dagster-dbt. So the thing worth testing is not that the code calls the right
API, it is that the manifest and the asset graph agree on how many checks
there are and where they hang.

**Freshness.** That derivation happens once, at *definition* time, from
whatever `transform/target/manifest.json` is on disk. `dbt build` at run time
reads the project files instead. A manifest older than the project therefore
splits the two: dbt runs the tests, and Dagster has no check key to record
them against. That is not hypothetical — it is the state a checkout is in
after pulling a commit that adds tests, which is exactly how the ten
`dbt_expectations` tests came in.

Owner: lane A1.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from orchestration.resources import (
    DBT_PROJECT_DIR,
    deps_are_missing,
    manifest_is_stale,
    prepare_dbt_manifest,
)

# --- Manifest freshness ----------------------------------------------------
#
# `manifest_is_stale` is a pure function of two paths so these run against a
# skeleton project in tmp_path: no dbt, no BigQuery, no network.


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """A dbt project skeleton: one model, one schema.yml, one parsed manifest.

    Only the paths `manifest_is_stale` reads have to be real — the files'
    contents never are.
    """
    project_dir = tmp_path / "transform"
    (project_dir / "models" / "marts").mkdir(parents=True)
    (project_dir / "models" / "marts" / "fct_orders.sql").write_text("select 1")
    (project_dir / "models" / "marts" / "schema.yml").write_text("version: 2")
    (project_dir / "dbt_project.yml").write_text("name: olist")

    (project_dir / "target").mkdir()
    (project_dir / "target" / "manifest.json").write_text("{}")
    return project_dir


def manifest_of(project_dir: Path) -> Path:
    return project_dir / "target" / "manifest.json"


def touch(path: Path, *, newer_than: Path) -> None:
    """Make `path` a second newer than `newer_than`.

    A whole second because a filesystem that stores mtimes at second
    granularity would otherwise report two files written in the same test as
    simultaneous, and the comparison under test is strict.
    """
    path.touch()
    reference = newer_than.stat().st_mtime
    import os

    os.utime(path, (reference + 1, reference + 1))


def test_an_edited_model_makes_the_manifest_stale(project: Path):
    """The case that motivated this: a pulled commit adds tests to a schema.yml
    and the manifest on disk still describes the project as it was."""
    touch(project / "models" / "marts" / "schema.yml", newer_than=manifest_of(project))

    assert manifest_is_stale(project, manifest_of(project))


def test_a_manifest_newer_than_the_project_is_fresh(project: Path):
    """The common case, and the one that has to stay cheap: `dbt parse` is
    several seconds, and paying it on every code-location load would make
    `pytest` and every `run_all.py` slower for nothing."""
    touch(manifest_of(project), newer_than=project / "models" / "marts" / "schema.yml")

    assert not manifest_is_stale(project, manifest_of(project))


def test_a_missing_manifest_is_stale(tmp_path: Path):
    """A fresh clone and a CI runner: `transform/target/` is gitignored, so
    there is nothing to compare against and the answer has to be "parse"."""
    project_dir = tmp_path / "transform"
    (project_dir / "models").mkdir(parents=True)

    assert manifest_is_stale(project_dir, project_dir / "target" / "manifest.json")


def test_dbt_own_output_does_not_make_its_manifest_stale(project: Path):
    """`target/` and `dbt_packages/` are excluded, and the exclusion is not
    tidiness — it is what stops the check from always firing.

    dbt writes `run_results.json` and the whole `compiled/` tree *after*
    `manifest.json` in the same invocation, and the installed packages are
    unpacked with their own timestamps. Comparing against those would report
    every manifest as stale the moment it was created, re-parsing on every
    single load.
    """
    touch(project / "target" / "run_results.json", newer_than=manifest_of(project))
    (project / "dbt_packages" / "dbt_expectations").mkdir(parents=True)
    touch(
        project / "dbt_packages" / "dbt_expectations" / "dbt_project.yml",
        newer_than=manifest_of(project),
    )

    assert not manifest_is_stale(project, manifest_of(project))


def test_the_committed_project_and_its_manifest_agree():
    """The end-to-end guard, and the one that would have caught this.

    `prepare_dbt_manifest()` is what the asset graph calls, so after it returns
    the manifest must describe the project as it is now. Edit a schema.yml,
    forget to re-parse, and this fails with the two mtimes rather than with
    twenty dropped check results in a run nobody is watching.
    """
    manifest = prepare_dbt_manifest()

    assert not manifest_is_stale(DBT_PROJECT_DIR, manifest)


# --- Installing what the manifest needs ------------------------------------
#
# A stale manifest is usually re-parsed and done with. The exception is a
# commit that adds a *package*: `dbt parse` then fails on a macro it cannot
# resolve, which is how `dbt_expectations` would have landed.


def test_a_newly_locked_package_has_to_be_installed(project: Path):
    """The `dbt_expectations` case.

    Pulling a commit that adds a package rewrites packages.yml and
    package-lock.yml, but `dbt_packages/` is gitignored and still holds
    yesterday's set. dagster's own `DbtProject.has_uninstalled_deps` only asks
    whether that directory *exists*, so it answers False here — and the parse
    that follows fails on an unresolvable macro rather than installing.
    """
    (project / "packages.yml").write_text("packages: []")
    (project / "dbt_packages" / "dbt_utils").mkdir(parents=True)
    touch(project / "package-lock.yml", newer_than=project / "dbt_packages")

    assert deps_are_missing(project)


def test_installed_packages_are_not_reinstalled(project: Path):
    """`dbt deps` reaches the package index, and the module docstring promises
    `uv run pytest` needs no network. Re-installing on every load would break
    that for a directory that is already correct."""
    (project / "packages.yml").write_text("packages: []")
    (project / "package-lock.yml").write_text("packages: []")
    (project / "dbt_packages" / "dbt_utils").mkdir(parents=True)
    touch(project / "dbt_packages", newer_than=project / "package-lock.yml")

    assert not deps_are_missing(project)


def test_a_project_with_packages_and_no_install_dir_needs_deps(project: Path):
    """A fresh clone: `dbt_packages/` is gitignored, so it is simply absent."""
    (project / "packages.yml").write_text("packages: []")

    assert deps_are_missing(project)


def test_a_project_without_packages_never_needs_deps(project: Path):
    """No packages.yml, nothing to install — and `dbt deps` would fail."""
    assert not deps_are_missing(project)


# --- Every dbt test is an asset check --------------------------------------


@pytest.fixture(scope="module")
def manifest_tests() -> dict[str, dict]:
    """The `resource_type: test` nodes of the real, freshly-parsed manifest."""
    manifest = json.loads(prepare_dbt_manifest().read_text())
    return {
        node["name"]: node
        for node in manifest["nodes"].values()
        if node["resource_type"] == "test"
    }


@pytest.fixture(scope="module")
def check_keys() -> dict[str, object]:
    """Asset check keys in the resolved graph, by check name."""
    from orchestration.definitions import defs

    return {key.name: key for key in defs.resolve_asset_graph().asset_check_keys}


def test_every_dbt_test_arrives_as_an_asset_check(manifest_tests, check_keys):
    """No test may execute in `dbt build` without a check to record it against.

    Asserted as a set difference rather than a count so the failure names the
    tests that went missing.
    """
    assert set(manifest_tests) - set(check_keys) == set()


def test_the_expectations_tests_guard_the_models_they_are_declared_on(
    manifest_tests, check_keys
):
    """The tier-2 assertions specifically (§7), keyed to the right asset.

    A test that resolves to *an* asset check is only half the requirement; the
    §7 claim is that the failure lands on the model that produced the data. The
    row-count assertion belongs to `fct_orders`, not to whatever dbt happened
    to run before it.
    """
    expectations = {
        name: node
        for name, node in manifest_tests.items()
        if (node.get("test_metadata") or {}).get("namespace") == "dbt_expectations"
    }
    assert expectations, "no dbt_expectations tests in the manifest"

    guarded = {
        check_keys[name].asset_key.to_user_string()
        for name in expectations
        if name in check_keys
    }
    assert guarded == {
        "marts/fct_orders",
        "marts/fct_payments",
        "marts/fct_reviews",
        "intermediate/int_order_lifecycle",
    }


def test_the_warn_severity_tests_survive_the_trip_into_dagster(manifest_tests, check_keys):
    """`severity: warn` is a documented decision (docs/data_quality_tests.md),
    not an accident: the lifecycle monotonicity violations are real historical
    source data, and blocking the build on them would be wrong.

    dagster-dbt maps a dbt `warn` onto `AssetCheckSeverity.WARN` at run time,
    which only helps if the tests are declared and wired in the first place.
    This is the half that can be asserted without a warehouse.
    """
    warns = {
        name
        for name, node in manifest_tests.items()
        if str(node["config"].get("severity", "")).lower() == "warn"
    }
    assert warns, "docs/data_quality_tests.md documents warn-severity tests; none are declared"
    assert warns <= set(check_keys)


def test_a_broken_raw_table_fails_a_check_on_the_raw_asset(check_keys):
    """Source tests count too — `enable_source_tests_as_checks` (§7).

    Without it the `_sources.yml` tests still run inside `dbt build`, but the
    first sign of a broken `olist_raw` table is a failure three models
    downstream in staging, which is the diagnosis problem §7 exists to avoid.
    """
    guarded = {
        key.asset_key.to_user_string()
        for key in check_keys.values()
        if key.asset_key.to_user_string().startswith("olist_raw/")
    }
    assert guarded == {"olist_raw/olist_customers_dataset", "olist_raw/olist_orders_dataset"}
