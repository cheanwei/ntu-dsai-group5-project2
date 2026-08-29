{{ config(cluster_by = ['customer_key']) }}

-- Grain: customer_unique_id (~96,096 rows).
--
-- The single most consequential modelling decision in the project (§5.2).
-- Keying on customer_id instead would produce ~99k one-time customers and the
-- false finding that Olist has no repeat business. A dbt test below asserts
-- the row count stays below the order count.
--
-- Geographic attributes are denormalised in from int_geolocation_deduped;
-- there is deliberately no dim_geography (§5.2).
-- Marts do no cleaning — shaping only, plus unknown-member handling (§6).
-- Owner: lane A2.

with history as (

    select * from {{ ref('int_customer_order_history') }}

),

geo as (

    select * from {{ ref('int_geolocation_deduped') }}

),

final as (

    select
        -- TODO(A2): customer_key = customer_unique_id, city/state, median
        -- lat/lng, RFM inputs.
        *

    from history

)

select * from final
