# Notebooks

Outputs are committed deliberately: GitHub renders a notebook from the blob in
the repository, so a notebook committed without outputs shows no charts. Re-run
a notebook before committing so its stored outputs match its code:

```bash
uv run jupyter nbconvert --to notebook --execute --inplace <notebook>.ipynb
```

Register the diff driver once per clone to keep notebook diffs readable:

```bash
git config diff.ipynb.textconv "uv run nbstripout -t"
```

`.gitattributes` declares `*.ipynb diff=ipynb`, but the command is stored in
local `.git/config` and is not copied by `git clone`. It strips outputs from
the *diff view* only and never rewrites the file.

## Current notebooks

| Notebook | Status | Data source |
|---|---|---|
| `01_data_profiling.ipynb` | primary analysis | BigQuery marts via SQLAlchemy |
| `02_sales_trends.ipynb` | exploratory prototype | local raw CSVs under `data/staging/`; its BigQuery write is guarded behind `LOAD_TO_BQ=1` |

Notebook 01 contains the implemented profiling and business analysis. Notebook
02 does not follow the platform's marts-only consumption rule and should be
refactored before its results are treated as canonical.

Keep cleaning and reusable business logic in dbt. Notebooks should query
`*_marts`, aggregate in BigQuery, and use pandas only for small result sets and
visualization.
