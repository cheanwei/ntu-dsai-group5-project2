-- Create a table ready for mapping visualization of Buyer's location and revenue
-- For Boon Wee & Willie's Dashboard

  {{ config(
    materialized='table',
    description="Revenue by City and State with Latitude and Longitude for Mapping"
            
  ) }}

SELECT 
  customer_city AS city,
  customer_state AS state,
  
  -- Average coordinates for plotting city markers on a map
  AVG(latitude) AS latitude,
  AVG(longitude) AS longitude,
  
  -- Metrics
  COUNT(DISTINCT order_id) AS total_orders,
  ROUND(SUM(total_order_revenue), 2) AS total_revenue_Brazilian_Real

FROM {{ source('olist_eCommerce', 'fact_sale_order_location') }}

-- Filter out records missing valid coordinates
WHERE latitude IS NOT NULL AND longitude IS NOT NULL

GROUP BY customer_city, customer_state
ORDER BY total_revenue_Brazilian_Real DESC