-- Per-customer order history, keyed on customer_unique_id.
--
-- The key is the point of this model. customer_id is a per-order surrogate;
-- rolling up on it yields ~99k customers who each ordered once and the false
-- conclusion that Olist has no repeat business. The true figure is ~96k unique
-- customers with roughly 3% repeating (§5.2). Any RFM segmentation built on
-- the wrong key is meaningless.
-- Owner: lane A2.

with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

history as (

    select
        -- TODO(A2): per customer_unique_id — first/last order date, order
        -- count, lifetime value, recency. Feeds the RFM notebook (§9).
        *

    from customers

)

select * from history
