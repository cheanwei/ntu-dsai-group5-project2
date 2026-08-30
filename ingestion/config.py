"""Load `ingestion/config.yml` into typed objects.

Design: architecture-design.md §4. The YAML holds what changes — the pinned
Kaggle version, the destination, the nine tables and their column hints — and
this module holds the shape those values must have, so a typo in the file fails
at load with the offending key named rather than as a KeyError inside a
resource loop.

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
# (§10). It is the only overridable value; `scripts/run_ingestion.py` names the
# raw bucket the same way, in `BUCKET_ENV`.
DATASET_ENV = "BIGQUERY_RAW_DATASET"

_TOP_LEVEL_KEYS = {"kaggle", "pipeline", "schema_contract", "tables"}
_TABLE_KEYS = {"file", "table", "text_columns", "timestamp_columns"}


class ConfigError(ValueError):
    """The config file is malformed. Raised with the offending key named."""


@dataclass(frozen=True)
class SourceTable:
    """One CSV, and the types that must be declared rather than inferred."""

    file: str
    table: str
    text_columns: tuple[str, ...] = ()
    timestamp_columns: tuple[str, ...] = ()

    @property
    def column_hints(self) -> dict[str, dict[str, str]]:
        """dlt column hints for this table."""
        return {
            **{col: {"data_type": "text"} for col in self.text_columns},
            **{col: {"data_type": "timestamp"} for col in self.timestamp_columns},
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


def _parse_table(raw: dict, index: int, filename: str) -> SourceTable:
    where = f"tables[{index}]"
    if not isinstance(raw, dict):
        raise ConfigError(f"{filename}: `{where}` is not a mapping")
    _reject_unknown(raw, _TABLE_KEYS, where, filename)
    return SourceTable(
        file=_require(raw, "file", where, filename),
        table=_require(raw, "table", where, filename),
        text_columns=tuple(raw.get("text_columns") or ()),
        timestamp_columns=tuple(raw.get("timestamp_columns") or ()),
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
