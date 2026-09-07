{{ config(cluster_by = ['date_key']) }}

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
    extract(year from date_day) as year_number,
    extract(quarter from date_day) as quarter_number,
    format_date('%Y-Q%Q', date_day) as year_quarter,
    extract(month from date_day) as month_number,
    format_date('%B', date_day) as month_name,
    format_date('%Y-%m', date_day) as year_month,
    extract(isoweek from date_day) as iso_week_number,
    extract(dayofweek from date_day) as day_of_week_number,
    format_date('%A', date_day) as day_name,
    extract(dayofweek from date_day) in (1, 7) as is_weekend,
    date_day = last_day(date_day, month) as is_month_end,
    date_day < date_add(date('{{ var("last_complete_month") }}'), interval 1 month)
        as is_complete_month

from spine
