#!/usr/bin/env python3
"""Generate the Olist high-level architecture diagram as Excalidraw.

The dimensional model is documented in ``docs/star_schema.md``.

Usage:  python generate_diagrams.py
"""

# Diagram labels are kept as readable, single string literals.
# ruff: noqa: E501

from pathlib import Path

from diagram_lib import Box, Diagram, Edge, emit

OUT = Path(__file__).parent

# Boxes whose first line names something get an enlarged, zone-coloured heading.
# Zone labels and the legend keep a single uniform size.
COMPONENT_BOXES = {
    "kaggle", "gcs", "dlt", "raw", "stg", "int", "marts",
    "dbttests", "gx", "nb", "streamlit", "powerbi", "dbtdocs", "gxdocs",
    "dagster",  # the orchestration band's first line is a heading like any other
}


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
            "Kaggle CSVs → GCS raw zone → dlt → BigQuery → dbt marts → notebooks + dashboards",
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
            "olist_marts  (tables)\nSTAR SCHEMA\n4 facts + 4 conformed dims\npartitioned + clustered\n→ see docs/star_schema.md",
            "mart",
            font=10,
        ),
        Box(
            "dbttests",
            580,
            462,
            170,
            78,
            "dbt built-ins\n+ dbt-utils\nunique · not_null\nrelationships · domains",
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
            195,
            210,
            80,
            "Jupyter + pandas\n01_data_profiling.ipynb\nSQLAlchemy BigQuery dialect\nqueries marts",
            "consume",
            font=10,
        ),
        Box(
            "streamlit",
            1195,
            300,
            210,
            80,
            "Streamlit dashboard\nmap via Flask /api/cities\nsunburst via direct SQL\ndashboards/dashboard.py",
            "consume",
            font=10,
        ),
        Box(
            "powerbi",
            1195,
            405,
            210,
            65,
            "Power BI report\nauthored outside this repo\nconnects to BigQuery marts",
            "consume",
            font=10,
        ),
        Box("dbtdocs", 1195, 495, 210, 50, "dbt docs\nlineage graph + catalog", "consume", font=10),
        Box(
            "gxdocs",
            1195,
            570,
            210,
            50,
            "Dagster asset checks\npass/fail per asset",
            "consume",
            font=10,
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
            "Solid = data flow   ·   Dashed = control / validation   ·   Exploratory notebook 02 (local CSVs) is excluded from data flow\n"
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
        Edge("f7", "marts", "nb", [(1095, 466), (1130, 466), (1130, 235), (1195, 235)], ""),
        Edge("f10", "marts", "streamlit", [(1095, 486), (1140, 486), (1140, 340), (1195, 340)], ""),
        Edge("f11", "marts", "powerbi", [(1095, 506), (1150, 506), (1150, 437), (1195, 437)], ""),
        Edge("f8", "marts", "dbtdocs", [(1095, 520), (1195, 520)], ""),
        Edge("f9", "gx", "gxdocs", [(750, 595), (1195, 595)], ""),
        Edge("q1", "dbttests", "stg", [(750, 480), (845, 300)], "", dashed=True),
        Edge("q2", "dbttests", "marts", [(750, 510), (845, 490)], "", dashed=True),
        Edge("q3", "marts", "gx", [(845, 540), (750, 575)], "validate", dashed=True),
        Edge("o1", "dagster", "kaggle", [(145, 700), (145, 310)], "", dashed=True),
        Edge("o2", "dagster", "gx", [(665, 700), (665, 627)], "", dashed=True),
        Edge("o3", "dagster", "marts", [(970, 700), (970, 551)], "", dashed=True),
    ]

    for box in b:
        box.head = box.id in COMPONENT_BOXES

    return Diagram("High-Level Architecture", 1470, 920, b, e)



if __name__ == "__main__":
    print("Generating diagrams into", OUT)
    emit(high_level_architecture(), "01-high-level-architecture", OUT)
    print("Done.")
