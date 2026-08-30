"""dlt source: the nine Olist CSVs, with explicit column hints.

Design: architecture-design.md §4. Three non-negotiables are enforced here; the
values they are enforced against live in ``config.yml``.

1. **Explicit hints, not schema inference.** ``customer_zip_code_prefix`` and
   ``seller_zip_code_prefix`` carry leading zeros; inferred as INT64 they are
   silently corrupted. Declared as ``text_columns`` in ``config.yml``, and
   defended twice — see the comments in the loop below for why one defence is
   not enough.
2. **``schema_contract`` set to freeze.** An unexpected column or a type change
   fails the load rather than silently reshaping the warehouse. This is the
   clearest single piece of evidence for the pipeline-integrity criterion, and
   it can be demonstrated live by feeding the loader a tenth column.
3. **Type enforcement only.** No dropping rows, no filling nulls, no renaming —
   raw must stay faithful to source (§6).

Owner: lane A1.
"""

from __future__ import annotations

import dlt
from dlt.sources.filesystem import filesystem, read_csv

from ingestion.config import IngestionConfig, config


@dlt.source(name="olist")
def olist_source(bucket_url: str, cfg: IngestionConfig | None = None):
    """A dlt source over the nine CSVs under ``bucket_url``.

    One ``filesystem(file_glob=<one file>) | read_csv()`` resource per table, so
    each arrives as its own dlt resource — and therefore its own Dagster asset
    in the lineage graph (§8). The glob names a single file rather than
    ``*.csv``: nine tables, not one union.
    """
    cfg = cfg or config()

    for table in cfg.tables:
        csv_file = filesystem(bucket_url=bucket_url, file_glob=table.file)
        # `dtype` is not belt-and-braces with the column hints below — it is the
        # half that actually saves the data. read_csv is pandas-backed, so an
        # unpinned parse turns "01234" into 1234 before dlt ever sees a hint.
        #
        # `dtype_backend` is load-bearing too, and timestamps are deliberately
        # NOT parsed here. pandas' own `parse_dates` yields datetime64 with NaT
        # for a missing value, and NaT reaches dlt as a text variant column —
        # which the frozen contract then rejects, failing the whole load on a
        # blank delivery date. Arrow-backed strings carry a real null, and the
        # timestamp hint below does the typing.
        resource = csv_file | read_csv(
            dtype={col: "string" for col in table.text_columns},
            dtype_backend="pyarrow",
        )

        resource = resource.with_name(table.table)
        # And the hints are the half that survives a load carrying no data for a
        # column: dlt infers types from values, so an all-null column without a
        # hint is dropped rather than typed.
        # The contract is applied per resource rather than on the @dlt.source
        # decorator, because the decorator binds its arguments at import and the
        # contract comes from config that is read at call time.
        resource.apply_hints(
            columns=table.column_hints,
            write_disposition="replace",
            schema_contract=cfg.schema_contract,
        )
        yield resource
