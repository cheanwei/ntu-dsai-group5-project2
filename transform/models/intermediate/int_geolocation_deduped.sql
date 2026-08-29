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
        -- TODO(A2): median lat/lng per prefix; drop coordinates outside
        -- Brazil's bounding box.
        *

    from geolocation

)

select * from deduped
