{{ config(cluster_by = ['date_day']) }}

-- One row per day, 2016-09-01 -> 2018-12-31 (~852 rows).
-- Conformed date dimension for all four facts.
-- Owner: lane A2.

with spine as (

    select date_day
    from unnest(generate_date_array(
        date('{{ var("dataset_start_date") }}'),
        date('{{ var("dataset_end_date") }}')
    )) as date_day

)

select
    date_day as date_key,
    -- TODO(A2): year, quarter, month, month_name, week, day_of_week,
    -- is_weekend, is_month_end. Flag the incomplete tail after
    -- var('last_complete_month') so trend charts can exclude it (§9).
    date_day

from spine
