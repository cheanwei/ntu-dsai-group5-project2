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
        -- TODO(A2): seller_key, city/state, median lat/lng.
        *

    from sellers

)

select * from final
