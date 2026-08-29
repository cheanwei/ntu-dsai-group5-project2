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
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parent / "config.yml"

# Environment variables that override a config value, and the field they set.
# The environment wins so CI and a per-developer sandbox can retarget the load
# without editing a committed file (§10).
ENV_OVERRIDES = {"dataset": "BIGQUERY_RAW_DATASET"}

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
        return os.environ.get(ENV_OVERRIDES["dataset"]) or self.configured_dataset

    @property
    def files(self) -> tuple[str, ...]:
        return tuple(t.file for t in self.tables)


def _require(mapping: dict, key: str, where: str):
    if key not in mapping:
        raise ConfigError(f"{CONFIG_PATH.name}: `{where}` is missing `{key}`")
    return mapping[key]


def _reject_unknown(mapping: dict, allowed: set[str], where: str) -> None:
    unknown = set(mapping) - allowed
    if unknown:
        raise ConfigError(
            f"{CONFIG_PATH.name}: `{where}` has unknown key(s) "
            f"{', '.join(sorted(unknown))}. Allowed: {', '.join(sorted(allowed))}"
        )


def _parse_table(raw: dict, index: int) -> SourceTable:
    where = f"tables[{index}]"
    if not isinstance(raw, dict):
        raise ConfigError(f"{CONFIG_PATH.name}: `{where}` is not a mapping")
    _reject_unknown(raw, _TABLE_KEYS, where)
    return SourceTable(
        file=_require(raw, "file", where),
        table=_require(raw, "table", where),
        text_columns=tuple(raw.get("text_columns") or ()),
        timestamp_columns=tuple(raw.get("timestamp_columns") or ()),
    )


def load_config(path: Path | str | None = None) -> IngestionConfig:
    """Read and validate the config file.

    Not cached, so a test can point it at a different file. Callers that read it
    at import time should use `config()` instead.
    """
    config_path = Path(path) if path is not None else CONFIG_PATH
    raw = yaml.safe_load(config_path.read_text()) or {}
    _reject_unknown(raw, _TOP_LEVEL_KEYS, "<root>")

    pipeline = _require(raw, "pipeline", "<root>")
    tables = tuple(
        _parse_table(entry, i) for i, entry in enumerate(_require(raw, "tables", "<root>"))
    )
    duplicates = {t.table for t in tables if [x.table for x in tables].count(t.table) > 1}
    if duplicates:
        raise ConfigError(
            f"{config_path.name}: duplicate table name(s) {', '.join(sorted(duplicates))}"
        )

    return IngestionConfig(
        kaggle_dataset=_require(_require(raw, "kaggle", "<root>"), "dataset", "kaggle"),
        pipeline_name=_require(pipeline, "name", "pipeline"),
        configured_dataset=_require(pipeline, "dataset", "pipeline"),
        location=_require(pipeline, "location", "pipeline"),
        schema_contract=dict(_require(raw, "schema_contract", "<root>")),
        tables=tables,
    )


@lru_cache(maxsize=1)
def _cached() -> IngestionConfig:
    return load_config()


def config() -> IngestionConfig:
    """The shipped config, read once per process."""
    return _cached()
