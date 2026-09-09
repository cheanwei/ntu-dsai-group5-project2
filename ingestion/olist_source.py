"""dlt source: the nine Olist CSVs, loaded against a fixed schema.

Design: architecture-design.md §4. Three non-negotiables are enforced here; the
values they are enforced against live in ``config.yml``.

1. **A fixed schema, not an inferred one.** Every column of every table is
   declared in ``config.yml`` with the type it must land as, and both halves of
   the load are derived from that one declaration — the pandas parse and the
   dlt column hints. Neither can drift from the other, and inference gets no
   say in any of the 52 columns.
2. **``schema_contract`` set to freeze.** An unexpected column or a type
   change fails the load rather than silently reshaping the warehouse. Because
   the schema is declared rather than discovered, this bites on the *first*
   load: there is no run in which dlt is still learning the shape. It can be
   demonstrated live by feeding the loader a tenth column.
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
    schema = dlt.current.source_schema()

    for table in cfg.tables:
        csv_file = filesystem(bucket_url=bucket_url, file_glob=table.file)
        # `dtype` is not belt-and-braces with the column hints below — it is the
        # half that actually saves the data. read_csv is pandas-backed, so an
        # unpinned parse turns "01234" into 1234 and a null-bearing integer
        # column into float64 before dlt ever sees a hint. Both maps come from
        # the same `columns:` block, so a column can only be typed once.
        #
        # `dtype_backend` is load-bearing too. It is what leaves the timestamp
        # columns — deliberately absent from `pandas_dtypes` — as Arrow strings
        # carrying a real null, rather than datetime64 with NaT. See
        # `config._DATA_TYPES` for why NaT would fail the load outright.
        resource = csv_file | read_csv(
            dtype=table.pandas_dtypes,
            dtype_backend="pyarrow",
        )

        resource = resource.with_name(table.table)
        # And the hints are the half that survives a load carrying no data for a
        # column: dlt infers types from values, so an all-null column without a
        # hint is dropped rather than typed. That is a property of the load,
        # not of the dataset — `review_comment_title` is ~88% null across the
        # full source and infers fine there, but any slice with no comment at
        # all would lose the column, and the raw table's shape would depend on
        # which rows happened to arrive.
        #
        # The contract is applied per resource rather than on the @dlt.source
        # decorator, because the decorator binds its arguments at import and the
        # contract comes from config that is read at call time.
        resource.apply_hints(
            columns=table.column_hints,
            write_disposition="replace",
            schema_contract=cfg.schema_contract,
        )
        # Seed the declaration into the source schema before anything is
        # extracted. Without this the contract still only bites on the *second*
        # run: dlt grants a table `evolve-columns-once` when it is absent from
        # the schema at extract time (`Schema.is_new_table`), and the hints
        # above are merged as part of that same first extract — too late to be
        # compared against. Writing them in up front means the table is already
        # there, complete, when the first row arrives, so a tenth column fails
        # the run that introduced it rather than the one after.
        schema.update_table(resource.compute_table_schema())
        yield resource
