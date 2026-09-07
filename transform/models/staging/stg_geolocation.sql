-- stg_geolocation: 1:1 with source `olist_raw.geolocation`.
-- Allowed here (§6): snake_case renames, casts, whitespace trim, single-table
-- dedupe. Prohibited here: cross-table logic, business rules, filtering rows
-- for business reasons.
-- ~1M rows, kept at source grain; the dedupe happens in intermediate (§5.2).
-- Owner: lane A2. Descriptions: lane C1.

with source as (

    select * from {{ source('olist_raw', 'geolocation') }}

),

renamed as (

    select
        trim(geolocation_zip_code_prefix) as geolocation_zip_code_prefix,
        cast(geolocation_lat as numeric) as geolocation_lat,
        cast(geolocation_lng as numeric) as geolocation_lng,
        lower(trim(geolocation_city)) as geolocation_city,
        upper(trim(geolocation_state)) as geolocation_state

    from source

)

select * from renamed
