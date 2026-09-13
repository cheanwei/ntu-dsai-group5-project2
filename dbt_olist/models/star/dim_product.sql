-- Create a new clean Products table

  {{ config(
    materialized='table',
    description="Dimensional table for the products"
            
  ) }}

  SELECT 
    -- Keep the unique product ID
    p.product_id,
    
    -- it replaces NULL by 'unknown'
    COALESCE(t.product_category_name_english, 'unknown') AS product_category_name_english,
    
    -- Keep the product dimensions and weight
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm

    FROM {{ source('olist_eCommerce_1', 'public_olist_products_dataset') }} AS p
  
  -- Use LEFT JOIN to keep ALL products, incase of missing translations
    LEFT JOIN {{ source('olist_eCommerce_1', 'public_product_category_name_translation') }} AS t
    ON p.product_category_name = t.product_category_name

