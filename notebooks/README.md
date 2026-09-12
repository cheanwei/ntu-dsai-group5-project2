# Notebooks

Install the repository's output-stripping filter once per clone:

```bash
uv run nbstripout --install
uv run nbstripout --status
```

`.gitattributes` declares the filter, but the executable path is stored in
local `.git/config` and is not copied by `git clone`.

## Current notebooks

| Notebook | Status | Data source |
|---|---|---|
| `01_data_profiling.ipynb` | primary analysis | BigQuery marts via SQLAlchemy |
| `02_sales_trends.ipynb` | exploratory prototype | local raw CSVs, with an experimental BigQuery write |

Notebook 01 contains the implemented profiling and business analysis. Notebook
02 does not follow the platform's marts-only consumption rule and should be
refactored before its results are treated as canonical.

Keep cleaning and reusable business logic in dbt. Notebooks should query
`*_marts`, aggregate in BigQuery, and use pandas only for small result sets and
visualization.
