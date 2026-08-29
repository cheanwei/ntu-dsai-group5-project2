# Seeds

`product_category_name_translation.csv` — the Portuguese-to-English category
mapping. Copy it here from the Kaggle download; it is ~71 rows and belongs in
git, unlike the 120 MB of transactional CSVs which live in the GCS raw zone
(§4).

    cp <kaggle-download>/product_category_name_translation.csv transform/seeds/
    dbt seed

The same file is also loaded to `olist_raw` by dlt. The seed exists so the
translation is legible and diffable in the repository; pick one as the source
for `dim_product` and reference only that one.

`.gitignore` excludes `*.csv` globally and re-admits `transform/seeds/*.csv`.
