-- Create a new clean Geolocation table

  {{ config(
    materialized='table',
    description="Dimensional table for the geolocation data"
    ) }}

SELECT 
    --Unique 5-digit Zip Code Prefix (Primary Key)
    geolocation_zip_code_prefix AS zip_code_prefix,
    
    --Take the average latitude and longitude for this zip code
    AVG(geolocation_lat) AS latitude,
    AVG(geolocation_lng) AS longitude,
    
    --Get the city and state for this zip code
    ANY_VALUE(geolocation_city) AS city,
    ANY_VALUE(geolocation_state) AS state

  FROM {{ source('olist_eCommerce_1', 'public_olist_geolocation_dataset') }}
  GROUP BY geolocation_zip_code_prefix