"""Load `ingestion/config.yml` into typed objects.

Design: architecture-design.md §4. The YAML holds what changes — the pinned
Kaggle version, the destination, and the nine tables with every column they
land with — and this module holds the shape those values must have, so a typo
in the file fails at load with the offending key named rather than as a
KeyError inside a resource loop.

Owner: lane A1.
"""

from __future__ import annotations

import os
from collections import Counter
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parent / "config.yml"

# Overrides `pipeline.dataset` where set. The environment wins so CI and a
# per-developer sandbox can retarget the load without editing a committed file
# (§10). It is the only overridable value; the raw bucket is named the same way
# in `orchestration/resources.py`, as `RAW_BUCKET_ENV`.
DATASET_ENV = "BIGQUERY_RAW_DATASET"

_TOP_LEVEL_KEYS = {"kaggle", "pipeline", "schema_contract", "tables"}
_TABLE_KEYS = {"file", "table", "columns"}

# The dlt data types this project declares. Deliberately a subset of dlt's full
# set: nine flat CSVs need four types, and a typo like `varchar` should be
# named at config load rather than surface as an inscrutable dlt error mid-run.
#
# The pandas dtype is what `read_csv` parses with, and it is derived from the
# same declaration as the dlt hint so the two cannot disagree. `timestamp` maps
# to None on purpose: pandas would yield datetime64 with NaT for a blank
# delivery date, and NaT reaches dlt as a text variant that the frozen contract
# then rejects, failing the whole load. Left as an Arrow-backed string it
# carries a real null, and the dlt hint does the typing.
_DATA_TYPES: dict[str, str | None] = {
    "text": "string",
    "bigint": "int64[pyarrow]",
    "double": "double[pyarrow]",
    "timestamp": None,
}


class ConfigError(ValueError):
    """The config file is malformed. Raised with the offending key named."""


@dataclass(frozen=True)
class SourceTable:
    """One CSV, and every column it lands with — declared, never inferred.

    `columns` maps source column name to dlt data type, in file order. It is
    exhaustive: the frozen contract rejects anything not named here, so a
    forgotten column fails the load rather than widening the table.
    """

    file: str
    table: str
    columns: dict[str, str] = field(default_factory=dict)

    @property
    def column_hints(self) -> dict[str, dict[str, object]]:
        """dlt column hints: the whole table schema, every column nullable.

        Nullable throughout because raw is faithful to source (§6) — the CSVs
        genuinely carry blank weights and undelivered dates, and a NOT NULL
        here would reject rows the source actually contains. not_null lives in
        dbt, on the columns where it holds.
        """
        return {
            col: {"data_type": data_type, "nullable": True}
            for col, data_type in self.columns.items()
        }

    @property
    def pandas_dtypes(self) -> dict[str, str]:
        """What `read_csv` must parse each column as.

        Timestamps are absent by design — see `_DATA_TYPES`.
        """
        return {
            col: _DATA_TYPES[data_type]
            for col, data_type in self.columns.items()
            if _DATA_TYPES[data_type] is not None
        }


@dataclass(frozen=True)
class IngestionConfig:
    kaggle_dataset: str
    pipeline_name: str
    configured_dataset: str
    location: str
    schema_contract: dict[str, str]
    tables: tuple[SourceTable, ...] = field(default_factory=tuple)

    @property
    def dataset(self) -> str:
        """Destination dataset, with the environment winning.

        Resolved on access rather than at load: `config()` is cached for the
        process, so freezing the environment into it would make a sandbox
        override depend on which module imported first.
        """
        return os.environ.get(DATASET_ENV) or self.configured_dataset

    @property
    def csv_filenames(self) -> tuple[str, ...]:
        """The nine source filenames, in config order — what the raw zone holds."""
        return tuple(t.file for t in self.tables)


# --- Validation ------------------------------------------------------------
#
# Every helper takes the file name being read so the message names that file.
# `load_config` accepts a path, so hardcoding the shipped name would misreport
# which file the bad key is actually in.


def _require(mapping: dict, key: str, where: str, filename: str):
    if key not in mapping:
        raise ConfigError(f"{filename}: `{where}` is missing `{key}`")
    return mapping[key]


def _reject_unknown(mapping: dict, allowed: set[str], where: str, filename: str) -> None:
    unknown = set(mapping) - allowed
    if unknown:
        raise ConfigError(
            f"{filename}: `{where}` has unknown key(s) "
            f"{', '.join(sorted(unknown))}. Allowed: {', '.join(sorted(allowed))}"
        )


def _reject_duplicate_tables(tables: tuple[SourceTable, ...], filename: str) -> None:
    counts = Counter(t.table for t in tables)
    duplicates = sorted(name for name, count in counts.items() if count > 1)
    if duplicates:
        raise ConfigError(f"{filename}: duplicate table name(s) {', '.join(duplicates)}")


def _parse_columns(raw: dict, table: str, filename: str) -> dict[str, str]:
    """Validate one table's `columns:` block.

    Insertion order is kept — YAML preserves it, and it is the CSV's own column
    order, which makes the declaration diffable against a `head -1` of the file.
    """
    columns = _require(raw, "columns", f"tables[{table}]", filename)
    if not isinstance(columns, dict) or not columns:
        raise ConfigError(f"{filename}: `{table}.columns` must be a non-empty mapping")

    for col, data_type in columns.items():
        if data_type not in _DATA_TYPES:
            raise ConfigError(
                f"{filename}: `{table}.{col}` has unknown data type {data_type!r}. "
                f"Allowed: {', '.join(sorted(_DATA_TYPES))}"
            )
    return dict(columns)


def _parse_table(raw: dict, index: int, filename: str) -> SourceTable:
    where = f"tables[{index}]"
    if not isinstance(raw, dict):
        raise ConfigError(f"{filename}: `{where}` is not a mapping")
    _reject_unknown(raw, _TABLE_KEYS, where, filename)
    table = _require(raw, "table", where, filename)
    return SourceTable(
        file=_require(raw, "file", where, filename),
        table=table,
        columns=_parse_columns(raw, table, filename),
    )


# --- Entry points ----------------------------------------------------------


def load_config(path: Path | str | None = None) -> IngestionConfig:
    """Read and validate the config file.

    Not cached, so a test can point it at a different file. Callers that read it
    at import time should use `config()` instead.
    """
    config_path = Path(path) if path is not None else CONFIG_PATH
    filename = config_path.name
    raw = yaml.safe_load(config_path.read_text()) or {}
    _reject_unknown(raw, _TOP_LEVEL_KEYS, "<root>", filename)

    kaggle = _require(raw, "kaggle", "<root>", filename)
    pipeline = _require(raw, "pipeline", "<root>", filename)
    tables = tuple(
        _parse_table(entry, index, filename)
        for index, entry in enumerate(_require(raw, "tables", "<root>", filename))
    )
    _reject_duplicate_tables(tables, filename)

    return IngestionConfig(
        kaggle_dataset=_require(kaggle, "dataset", "kaggle", filename),
        pipeline_name=_require(pipeline, "name", "pipeline", filename),
        configured_dataset=_require(pipeline, "dataset", "pipeline", filename),
        location=_require(pipeline, "location", "pipeline", filename),
        schema_contract=dict(_require(raw, "schema_contract", "<root>", filename)),
        tables=tables,
    )


@lru_cache(maxsize=1)
def config() -> IngestionConfig:
    """The shipped config, read once per process."""
    return load_config()
