#!/usr/bin/env python3
"""Generate the Olist data-platform diagrams in draw.io and Excalidraw formats.

Two intentionally decoupled views:
  01  high-level architecture   -- how data moves between systems (exec audience)
  02  warehouse + dbt design    -- how data is modelled inside BigQuery (technical)

Usage:  python generate_diagrams.py
"""

# Diagram labels are kept as readable, single string literals.
# ruff: noqa: E501

from pathlib import Path

from diagram_lib import Box, Diagram, Edge, emit

OUT = Path(__file__).parent


# =============================================================== DIAGRAM 1 ===
def high_level_architecture() -> Diagram:
    b, e = [], []

    b.append(
        Box(
            "title",
            40,
            30,
            1390,
            50,
            "Olist Brazilian E-Commerce  ·  Data Platform Architecture\n"
            "Kaggle CSVs → GCS raw zone → dlt → BigQuery → dbt marts → notebook analysis",
            "note",
            font=14,
        )
    )

    # -- zones -----------------------------------------------------------
    b += [
        Box("z1", 40, 170, 210, 190, "1 · SOURCE", "zone", zone=True, font=11),
        Box("z2", 300, 170, 210, 190, "2 · RAW ZONE", "zone", zone=True, font=11),
        Box("z3", 560, 170, 210, 190, "3 · INGESTION (EL)", "zone", zone=True, font=11),
        Box("z4", 820, 110, 300, 530, "4 · WAREHOUSE — BigQuery", "zone", zone=True, font=11),
        Box("z5", 1170, 170, 260, 470, "5 · CONSUMPTION", "zone", zone=True, font=11),
        Box("z6", 560, 425, 210, 215, "6 · DATA QUALITY", "zone", zone=True, font=11),
    ]

    # -- nodes -----------------------------------------------------------
    b += [
        Box(
            "kaggle",
            55,
            210,
            180,
            100,
            "Kaggle\nolistbr/\nbrazilian-ecommerce\n9 CSVs · ~120 MB\npinned by version",
            "source",
            font=10,
        ),
        Box(
            "gcs",
            315,
            210,
            180,
            100,
            "Cloud Storage\ngs://<raw-bucket>/\nYYYY-MM-DD/\nreplayable raw files",
            "storage",
            font=10,
        ),
        Box(
            "dlt",
            575,
            210,
            180,
            100,
            "dlt pipeline\nfixed schema (52 cols)\nschema contract: freeze\nwrite_disposition:\nreplace (idempotent)",
            "move",
            font=10,
        ),
        Box(
            "raw",
            845,
            165,
            250,
            72,
            "olist_raw\n1:1 with source · typed\n+ _dlt_loads lineage",
            "wh",
            font=10,
        ),
        Box(
            "stg",
            845,
            262,
            250,
            72,
            "olist_staging  (views)\nrename · cast · dedupe\none model per source",
            "wh",
            font=10,
        ),
        Box(
            "int",
            845,
            359,
            250,
            72,
            "olist_intermediate  (views)\ngeo dedupe · order lifecycle\nreusable business logic",
            "wh",
            font=10,
        ),
        Box(
            "marts",
            845,
            456,
            250,
            95,
            "olist_marts  (tables)\nSTAR SCHEMA\n4 facts + 4 conformed dims\npartitioned + clustered\n→ see diagram 02",
            "mart",
            font=10,
        ),
        Box(
            "dbttests",
            580,
            462,
            170,
            78,
            "dbt built-ins + dbt-utils\nunique · not_null\nrelationships · domains",
            "quality",
            font=10,
        ),
        Box(
            "gx",
            580,
            555,
            170,
            72,
            "dbt-expectations\nranges · value sets\ntimestamp ordering",
            "quality",
            font=10,
        ),
        Box(
            "nb",
            1195,
            200,
            210,
            100,
            "Jupyter + pandas\n01_data_profiling.ipynb\nSQLAlchemy BigQuery dialect\nqueries marts",
            "consume",
            font=10,
        ),
        Box("dbtdocs", 1195, 325, 210, 75, "dbt docs\nlineage graph + catalog", "consume", font=10),
        Box(
            "gxdocs",
            1195,
            425,
            210,
            75,
            "Dagster asset checks\npass/fail per asset",
            "consume",
            font=10,
        ),
        Box(
            "prototypes",
            1195,
            525,
            210,
            100,
            "Prototypes (not connected)\nnotebook 02: local CSVs\nStreamlit: missing API route\nPower BI: no report",
            "note",
            font=9,
        ),
        Box(
            "dagster",
            40,
            700,
            1390,
            112,
            "ORCHESTRATION — Dagster (software-defined assets)\n"
            "dlt assets  →  @dbt_assets (one Dagster asset auto-generated per dbt model)  →  dbt tests as asset checks\n"
            "`dagster dev` locally   ·   webserver + daemon in Docker Compose on a GCP e2-micro VM   ·   daemon holds the daily schedule\n"
            "GitHub Actions builds and deploys the image; UI reached over an IAP tunnel (ingress is IAP-only)",
            "orch",
            font=11,
        ),
        Box(
            "legend",
            40,
            830,
            1390,
            62,
            "Solid = data flow   ·   Dashed = control / validation   ·   Unfinished prototypes are excluded from data flow\n"
            "The raw zone makes loading and transformation repeatable without downloading from Kaggle again",
            "note",
            font=10,
        ),
    ]

    # -- edges -----------------------------------------------------------
    e += [
        Edge("f1", "kaggle", "gcs", [(235, 260), (315, 260)], "kagglehub"),
        Edge("f2", "gcs", "dlt", [(495, 260), (575, 260)], "read CSV"),
        Edge("f3", "dlt", "raw", [(755, 260), (800, 260), (800, 201), (845, 201)], "load job"),
        Edge("f4", "raw", "stg", [(970, 237), (970, 262)], "dbt"),
        Edge("f5", "stg", "int", [(970, 334), (970, 359)], "dbt"),
        Edge("f6", "int", "marts", [(970, 431), (970, 456)], "dbt"),
        Edge("f7", "marts", "nb", [(1095, 466), (1140, 466), (1140, 246), (1195, 246)], "SQL"),
        Edge("f8", "marts", "dbtdocs", [(1095, 500), (1148, 500), (1148, 362), (1195, 362)], ""),
        Edge("f9", "gx", "gxdocs", [(750, 591), (1172, 591), (1172, 462), (1195, 462)], ""),
        Edge("q1", "dbttests", "stg", [(750, 480), (845, 300)], "", dashed=True),
        Edge("q2", "dbttests", "marts", [(750, 510), (845, 490)], "", dashed=True),
        Edge("q3", "marts", "gx", [(845, 540), (750, 575)], "validate", dashed=True),
        Edge("o1", "dagster", "kaggle", [(145, 700), (145, 310)], "", dashed=True),
        Edge("o2", "dagster", "gx", [(665, 700), (665, 627)], "", dashed=True),
        Edge("o3", "dagster", "marts", [(970, 700), (970, 551)], "", dashed=True),
    ]

    return Diagram("High-Level Architecture", 1470, 920, b, e)


# =============================================================== DIAGRAM 2 ===
def warehouse_design() -> Diagram:
    b, e = [], []

    b.append(
        Box(
            "title",
            40,
            30,
            1480,
            50,
            "Data Warehouse Design  ·  dbt layering + dimensional model (BigQuery)\n"
            "Fact constellation: fct_order_items is the atomic sales fact; three companion facts share the conformed dimensions",
            "note",
            font=14,
        )
    )

    # -- dbt layer band ---------------------------------------------------
    b.append(Box("zl", 40, 105, 1480, 175, "dbt PROJECT LAYERS", "zone", zone=True, font=11))
    b += [
        Box(
            "l1",
            70,
            145,
            250,
            115,
            "SOURCES  (9)\nolist_raw.*\ndlt-loaded, typed\nfreshness tests",
            "layer",
            font=10,
        ),
        Box(
            "l2",
            360,
            145,
            250,
            115,
            "STAGING  (9 views)\nstg_*\n1:1 with source\nrename · cast · dedupe\ncasting lives here only",
            "layer",
            font=10,
        ),
        Box(
            "l3",
            650,
            145,
            250,
            115,
            "INTERMEDIATE  (4 views)\nint_*\ngeolocation dedupe\norder lifecycle pivot\npayment reconciliation",
            "layer",
            font=10,
        ),
        Box(
            "l4",
            940,
            145,
            250,
            115,
            "MARTS  (8 tables)\ndim_*  ·  fct_*\npartitioned + clustered\nthe only layer analysts\nare expected to query",
            "mart",
            font=10,
        ),
        Box(
            "l5",
            1230,
            145,
            260,
            115,
            "TESTS\ndbt built-ins + dbt-utils\ndbt-expectations\nkeys · ranges · domains\ntimestamp ordering",
            "quality",
            font=10,
        ),
    ]
    e += [
        Edge("lf1", "l1", "l2", [(320, 202), (360, 202)]),
        Edge("lf2", "l2", "l3", [(610, 202), (650, 202)]),
        Edge("lf3", "l3", "l4", [(900, 202), (940, 202)]),
        Edge("lf4", "l4", "l5", [(1190, 202), (1230, 202)]),
    ]

    # -- dimensional model ------------------------------------------------
    b.append(
        Box("zs", 40, 300, 1480, 990, "DIMENSIONAL MODEL — olist_marts", "zone", zone=True, font=11)
    )

    b += [
        Box(
            "dim_date",
            610,
            345,
            310,
            160,
            "dim_date   852 rows\n\ndate_key (PK)  DATE\nyear_number · quarter_number\nyear_month · iso_week_number\nday_name · is_weekend\nis_complete_month\n2016-09-01 → 2018-12-31",
            "dim",
            font=10,
            align="left",
        ),
        Box(
            "dim_customer",
            100,
            560,
            300,
            250,
            "dim_customer   ~96,096 rows\n\ncustomer_key (PK)\n  ← customer_unique_id\n  ⚠ NOT customer_id, which is a\n  per-order surrogate\ncustomer_city · customer_state\ncustomer_zip_code_prefix\ncustomer_latitude · customer_longitude\nfirst/last_order_date · order_count\nlifetime_revenue · recency_days",
            "dim",
            font=10,
            align="left",
        ),
        Box(
            "fct_order_items",
            590,
            580,
            350,
            250,
            "fct_order_items    ATOMIC SALES FACT\ngrain: one order line\n(order_id + order_item_id) · ~112,650\n\nFK  purchase date   → dim_date\nFK  customer_key    → dim_customer\nFK  product_key     → dim_product\nFK  seller_key      → dim_seller\nDD  order_id · order_item_id\n\nprice · freight_value\nline_gross_value = price + freight_value\n\nPARTITION BY order_purchase_date\nCLUSTER BY product_key, seller_key",
            "fact",
            font=10,
            align="left",
        ),
        Box(
            "dim_product",
            1130,
            560,
            310,
            210,
            "dim_product   ~32,951 rows\n\nproduct_key (PK)  ← product_id\nproduct_category_name\nproduct_category_name_english\nproduct_weight_g\nproduct_length/height/width_cm\nproduct_photos_qty\nproduct_name/description_length",
            "dim",
            font=10,
            align="left",
        ),
        Box(
            "dim_seller",
            1130,
            830,
            310,
            170,
            "dim_seller   ~3,095 rows\n\nseller_key (PK)  ← seller_id\nseller_city · seller_state\nseller_zip_code_prefix\nseller_latitude · seller_longitude",
            "dim",
            font=10,
            align="left",
        ),
        Box(
            "fct_orders",
            560,
            920,
            310,
            230,
            "fct_orders   ACCUMULATING SNAPSHOT\ngrain: one order · ~99,441\n\nFK  order_purchase_date → dim_date\nFK  customer_key        → dim_customer\nDD  order_id · order_status\n\nitem_count · payment_count\nitem_gross_total · payment_value_total\npayment_difference · reconciled flag\napproval_days · handover_days\ndelivery_days · estimated_vs_actual_days\nis_late",
            "fact",
            font=10,
            align="left",
        ),
        Box(
            "fct_payments",
            140,
            900,
            300,
            180,
            "fct_payments\ngrain: order_id +\n  payment_sequential · ~103,886\n\nDD  order_id  → fct_orders\npayment_type · installments\npayment_value\n\n⚠ kept separate — joining to\norder_items fans out revenue",
            "fact",
            font=10,
            align="left",
        ),
        Box(
            "fct_reviews",
            960,
            1060,
            300,
            175,
            "fct_reviews\ngrain: one review · ~99,224\n\nDD  order_id  → fct_orders\nFK  review_creation_date → dim_date\nreview_score (1–5)\nreview_response_days · has_comment\n\n⚠ review_id is NOT unique in\nthe source — dedupe in staging",
            "fact",
            font=10,
            align="left",
        ),
        Box(
            "note_geo",
            100,
            1110,
            400,
            140,
            "Why no dim_geography\nRaw geolocation is ~1M rows / ~19k zip prefixes.\nint_geolocation_deduped takes the median lat/lng per\nprefix, then the attributes are denormalised into\ndim_customer and dim_seller rather than kept as an\noutrigger: columnar storage compresses the repetition,\nand it removes a join from every geographic query.",
            "note",
            font=10,
            align="left",
        ),
        Box(
            "legend2",
            520,
            1180,
            380,
            95,
            "Solid arrow   = FK to a conformed dimension\nDashed arrow  = degenerate order_id link\nPK = primary key · FK = foreign key · DD = degenerate\n\nPartitioning is ~free at 120 MB; it is declared because\nthe grain and access pattern would demand it at 1000×.",
            "note",
            font=9,
            align="left",
        ),
    ]

    e += [
        Edge("s1", "dim_date", "fct_order_items", [(765, 505), (765, 580)]),
        Edge("s2", "dim_customer", "fct_order_items", [(400, 685), (590, 690)]),
        Edge("s3", "dim_product", "fct_order_items", [(1130, 665), (940, 680)]),
        Edge("s4", "dim_seller", "fct_order_items", [(1130, 890), (945, 800)]),
        Edge("s5", "dim_customer", "fct_orders", [(400, 770), (560, 965)]),
        Edge(
            "s6", "fct_orders", "fct_order_items", [(715, 920), (760, 830)], "order_id", dashed=True
        ),
        Edge(
            "s7", "fct_payments", "fct_orders", [(440, 1000), (560, 1010)], "order_id", dashed=True
        ),
        Edge(
            "s8", "fct_reviews", "fct_orders", [(960, 1120), (870, 1090)], "order_id", dashed=True
        ),
    ]

    return Diagram("Warehouse and Dimensional Model", 1560, 1320, b, e)


if __name__ == "__main__":
    print("Generating diagrams into", OUT)
    emit(high_level_architecture(), "01-high-level-architecture", OUT)
    emit(warehouse_design(), "02-warehouse-dimensional-model", OUT)
    print("Done.")
