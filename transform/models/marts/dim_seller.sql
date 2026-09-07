{{ config(cluster_by = ['seller_key']) }}

-- Grain: seller_id (~3,095 rows).
-- Geographic attributes denormalised in from int_geolocation_deduped (§5.2).
-- Owner: lane A2.

with sellers as (

    select * from {{ ref('stg_sellers') }}

),

geo as (

    select * from {{ ref('int_geolocation_deduped') }}

),

final as (

    select
        sellers.seller_id as seller_key,
        sellers.seller_city,
        sellers.seller_state,
        sellers.seller_zip_code_prefix,
        geo.geolocation_lat as seller_latitude,
        geo.geolocation_lng as seller_longitude

    from sellers
    left join geo
        on sellers.seller_zip_code_prefix = geo.geolocation_zip_code_prefix

)

select * from final
