-- Create a location-enriched Fact orders table with Payment Values

  {{ config(
    materialized='table',
    description="Buyer's latitude and longitude to every delivered order"
            
  ) }}

  SELECT 
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    UPPER(c.customer_state) AS customer_state,
    
    -- Add geographic coordinates
    g.latitude,
    g.longitude,
    
    SUM(p.payment_value) AS total_order_revenue,
    SAFE_CAST(o.order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp

  FROM {{ source('olist_eCommerce_1', 'public_olist_orders_dataset') }} AS o
  
-- Join customer details
  INNER JOIN {{ source('olist_eCommerce_1', 'public_olist_customers_dataset') }} AS c
    ON o.customer_id = c.customer_id
    
-- Join payment details
  INNER JOIN {{ source('olist_eCommerce_1', 'public_olist_order_payments_dataset') }} AS p
    ON o.order_id = p.order_id
    
-- Left Join dim deduplicated geolocation table to attach Lat/Lng
  LEFT JOIN {{ source('olist_eCommerce', 'dim_geolocation') }} AS g
    ON c.customer_zip_code_prefix = g.zip_code_prefix
    
-- Filter for delivered orders only
  WHERE o.order_status = 'delivered'
  
-- Group by non-aggregated fields
  GROUP BY 
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    g.latitude,
    g.longitude,
    o.order_purchase_timestamp