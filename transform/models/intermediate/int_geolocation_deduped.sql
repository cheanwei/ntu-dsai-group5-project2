-- One median lat/lng per zip prefix: ~1M raw rows -> ~19k prefixes.
-- Also removes out-of-Brazil coordinates. Cross-table reconciliation belongs
-- here, not in staging and not in a mart (§6).
-- Denormalised into dim_customer and dim_seller rather than kept as an
-- outrigger — there is deliberately no dim_geography (§5.2).
-- Owner: lane A2.

with geolocation as (

    select * from {{ ref('stg_geolocation') }}

),

deduped as (

    select
        geolocation_zip_code_prefix,
        percentile_cont(geolocation_lat, 0.5) over (
            partition by geolocation_zip_code_prefix
        ) as geolocation_lat,
        percentile_cont(geolocation_lng, 0.5) over (
            partition by geolocation_zip_code_prefix
        ) as geolocation_lng,
        row_number() over (
            partition by geolocation_zip_code_prefix
            order by geolocation_lat, geolocation_lng
        ) as row_number

    from geolocation
    -- Broad geographic bounds retain Brazil and remove clearly invalid points.
    where geolocation_lat between -35 and 6
      and geolocation_lng between -75 and -34

)

select * except (row_number)
from deduped
where row_number = 1
