# Macros

Shared SQL helpers. Keep them few — a macro that exists to hide business logic
puts that logic outside the layer that owns it (§6).

Likely candidates:

- `cents_to_brl` / consistent monetary rounding.
- `median_by_group` — used by `int_geolocation_deduped`.
- A `generate_schema_name` override, if per-developer targets need custom
  dataset naming beyond the `dbt run --target dev_<name>` convention (§10).
