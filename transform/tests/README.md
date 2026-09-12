# Singular dbt tests

No singular SQL tests are currently defined. The project's 79 checks live in
source and model `schema.yml` files and run during `dbt build`.

Add a SQL file here only when an invariant cannot be expressed clearly as a
generic dbt, dbt-utils, or dbt-expectations test. A singular test must return
the rows that violate the rule; an empty result passes.

See [the quality inventory](../../docs/data_quality_tests.md).
