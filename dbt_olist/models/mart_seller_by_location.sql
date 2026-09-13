-- Create a table ready for mapping visualization of Seller's location and revenue
-- For Boon Wee & Willie's Dashboard

  {{ config(
    materialized='table',
    description="Seller and Product Category (English) with Latitude and Longitude for Mapping"
            
  ) }}

SELECT 
  seller_id,
  product_category_name_english,
  city,
  state, 
  AVG(latitude) AS latitude,
  AVG(longitude) AS longitude,
  COUNT(DISTINCT order_id) AS total_orders,
  ROUND(SUM(total_revenue), 2) AS total_revenue_Brazilian_Real

FROM {{ source('olist_eCommerce', 'fact_revenue_by_product_seller_location') }}
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
GROUP BY
    seller_id,
    product_category_name_english,
    city,
    state
ORDER BY total_revenue_Brazilian_Real DESC
