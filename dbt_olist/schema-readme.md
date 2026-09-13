# Schema & Data Mart Architecture

This document details the Star Schema and Data Mart architecture for the Brazilian E-Commerce Dataset (Olist). The architecture separates granular transactional fact data from descriptive dimensions, enabling efficient analytical queries, spatial mapping, and executive reporting.

---

## 1. Data Lineage & Flow Connections

The lineage explicitly illustrates that **Fact Tables (3) and (4) pull transactional data directly from the RAW OLIST DATASETS** (such as `olist_order_items_dataset`, `olist_orders_dataset`, `olist_customers_dataset`, `olist_sellers_dataset`, and `olist_order_payments_dataset`), while simultaneously ingesting spatial coordinates and category translations from **(1) `dim_geolocation`** and **(2) `dim_product`**:

1. **Dimensions**:
   - **(1) `dim_geolocation`**: Built from raw `olist_geolocation_dataset` (aggregated to average lat/lng per 5-digit zip code).
   - **(2) `dim_product`**: Built from raw `olist_products_dataset` merged with `product_category_name_translation`.
2. **Fact Tables**:
   - **(3) `fact_revenue_by_product_seller_location`**: Ingests line-item transaction records directly from **RAW OLIST DATASETS** (`olist_order_items_dataset`, `olist_orders_dataset`, `olist_sellers_dataset`) and enriches them with coordinates from **(1)** and category names from **(2)**.
   - **(4) `fact_sale_order_location`**: Ingests order-level transaction records directly from **RAW OLIST DATASETS** (`olist_orders_dataset`, `olist_customers_dataset`, `olist_order_payments_dataset`) and enriches them with buyer coordinates from **(1)** and product attributes from **(2)**.
3. **Data Marts**:
   - **(3)** links with **(1)** and **(2)** to aggregate **(5) `mart_revenue_by_location`**.
   - **(4)** links with **(1)** and **(2)** to aggregate **(6) `mart_seller_by_location`**.

```text
                             RAW OLIST DATASETS
       ┌───────────────────┬──────────┴──────────┬───────────────────┐
       │                   │                     │                   │
       ▼                   ▼                     │                   │
(1) dim_geolocation   (2) dim_product            │                   │
(Deduplicated Lat/Lng)(English Categories)       │                   │
       │                   │                     │                   │
       │                   ├─────────────────────┼───────────────────┤
       │  Enrich Lat/Lng   │ Enrich Categories   │                   │
       ▼                   ▼                     ▼                   ▼
┌───────────────────────────────────────┐   ┌───────────────────────────────────┐
│ (3) fact_revenue_by_product_seller_loc│   │ (4) fact_sale_order_location      │
└───────────────────┬───────────────────┘   └─────────────────┬─────────────────┘
                    │                                         │
                    │ JOIN (1) + (2)                          │ JOIN (1) + (2)
                    ▼                                         ▼
┌───────────────────────────────────────┐   ┌───────────────────────────────────┐
│ (5) mart_revenue_by_location          │   │ (6) mart_seller_by_location       │
└───────────────────────────────────────┘   └───────────────────────────────────┘
```

---

## 2. Detailed Table Specifications

### (1) `dim_geolocation` (Spatial Dimension Table)
*Stores deduplicated 5-digit postal code coordinates (`AVG(geolocation_lat)`, `AVG(geolocation_lng)`) generated from `olist_geolocation_dataset`.*

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| **`zip_code_prefix`** | `STRING` | **Primary Key** | Deduplicated 5-digit zip code prefix. |
| **`latitude`** | `FLOAT64` | Attribute | Calculated average latitude (`AVG(geolocation_lat)`). |
| **`longitude`** | `FLOAT64` | Attribute | Calculated average longitude (`AVG(geolocation_lng)`). |
| **`city`** | `STRING` | Attribute | City name (`ANY_VALUE(geolocation_city)`). |
| **`state`** | `STRING` | Attribute | 2-letter state code (`ANY_VALUE(geolocation_state)`). |

---

### (2) `dim_product` (Product Dimension Table)
*Contains product catalog information and English category translations derived from `olist_products_dataset` and `product_category_name_translation`.*

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| **`product_id`** | `STRING` | **Primary Key** | Unique product identifier. |
| **`product_category_name_english`** | `STRING` | Attribute | English translation (e.g., `bed_bath_table`, `health_beauty`). |
| **`product_weight_g`** | `INT64` | Attribute | Product weight in grams. |
| **`product_length_cm`** | `INT64` | Attribute | Package length in cm. |
| **`product_height_cm`** | `INT64` | Attribute | Package height in cm. |
| **`product_width_cm`** | `INT64` | Attribute | Package width in cm. |

---

### (3) `fact_revenue_by_product_seller_location` (Transaction Item Fact Table)
*Granular item-level fact table created from **RAW OLIST DATASETS** (`olist_order_items_dataset`, `olist_orders_dataset`, `olist_sellers_dataset`) and enriched by **(1) `dim_geolocation`** and **(2) `dim_product`**.*

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| **`order_id`** | `STRING` | Foreign Key | Transaction order identifier. |
| **`order_item_id`** | `INT64` | Primary Key | Item line number within the order. |
| **`product_id`** | `STRING` | Foreign Key (from 2) | Links to `dim_product.product_id`. |
| **`seller_id`** | `STRING` | Foreign Key | Merchant identifier. |
| **`seller_zip_code_prefix`** | `STRING` | Foreign Key (from 1) | Links to `dim_geolocation.zip_code_prefix`. |
| **`price`** | `FLOAT64` | Metric | Product item price in BRL. |
| **`freight_value`** | `FLOAT64` | Metric | Shipping charge in BRL. |
| **`total_revenue`** | `FLOAT64` | Metric | Derived transaction value (`price + freight_value`). |

---

### (4) `fact_sale_order_location` (Customer Order Location Fact Table)
*Order-level fact table created from **RAW OLIST DATASETS** (`olist_orders_dataset`, `olist_customers_dataset`, `olist_order_payments_dataset`) and enriched by **(1) `dim_geolocation`** and **(2) `dim_product`**.*

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| **`order_id`** | `STRING` | Primary Key | Unique order identifier. |
| **`customer_id`** | `STRING` | Foreign Key | Transactional customer ID. |
| **`customer_unique_id`** | `STRING` | Foreign Key | Persistent customer identifier for repurchase tracking. |
| **`customer_zip_code_prefix`** | `STRING` | Foreign Key (from 1) | Links to `dim_geolocation.zip_code_prefix`. |
| **`product_id`** | `STRING` | Foreign Key (from 2) | Links to `dim_product.product_id`. |
| **`total_order_revenue`** | `FLOAT64` | Metric | Total payment value across all payment methods. |
| **`order_purchase_timestamp`** | `TIMESTAMP` | Metric / Attr | Date and time order was placed. |

---

### (5) `mart_revenue_by_location` (Location Summary Data Mart)
*Pre-aggregated data mart built by linking **(3)** `fact_revenue_by_product_seller_location` with **(1)** `dim_geolocation` and **(2)** `dim_product`.*

| Column Name | Data Type | Metric Type | Description |
| :--- | :--- | :--- | :--- |
| **`city`** | `STRING` | Dimension | Destination / Origin City Name. |
| **`state`** | `STRING` | Dimension | 2-letter State Code. |
| **`latitude`** | `FLOAT64` | Spatial Key | Map marker Y-coordinate. |
| **`longitude`** | `FLOAT64` | Spatial Key | Map marker X-coordinate. |
| **`total_orders`** | `INT64` | Aggregated Metric | Total count of distinct orders. |
| **`total_revenue_Brazilian_Real`** | `FLOAT64` | Aggregated Metric | Total revenue generated in BRL. |

---

### (6) `mart_seller_by_location` (Seller & Category Spatial Data Mart)
*Pre-aggregated data mart built by linking **(4)** `fact_sale_order_location` with **(1)** `dim_geolocation` and **(2)** `dim_product`.*

| Column Name | Data Type | Metric Type | Description |
| :--- | :--- | :--- | :--- |
| **`product_category_name_english`** | `STRING` | Dimension | English product category name. |
| **`seller_id`** | `STRING` | Dimension | Merchant identifier. |
| **`city`** | `STRING` | Dimension | Seller city. |
| **`state`** | `STRING` | Dimension | Seller 2-letter state code. |
| **`latitude`** | `FLOAT64` | Spatial Key | Seller map latitude coordinate. |
| **`longitude`** | `FLOAT64` | Spatial Key | Seller map longitude coordinate. |
| **`total_orders`** | `INT64` | Aggregated Metric | Distinct count of fulfilled orders. |
| **`total_revenue`** | `FLOAT64` | Aggregated Metric | Total order value in BRL. |

---

## 3. Key Architecture & Design Choices

- **Direct Ingestion into Fact Tables**: Fact tables **(3)** and **(4)** ingest core transactional records directly from the **RAW OLIST DATASETS**, while drawing spatial coordinates from **(1)** and translated categories from **(2)**.
- **Geolocation Deduplication**: Aggregating the 1M+ row raw geolocation table to unique 5-digit postal code averages prevents severe fan-out join errors.
- **Customer ID vs. Persistent Unique ID**: Using `customer_unique_id` preserves persistent buyer identity for RFM analysis, CLV, and customer churn modeling.
