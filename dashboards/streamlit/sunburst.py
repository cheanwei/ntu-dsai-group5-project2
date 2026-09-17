import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Revenue by Top Product", layout="wide")

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

PROJECT_ID = os.getenv("GCP_PROJECT", "dsai6mod2")
MARTS_DATASET = os.getenv("BIGQUERY_MARTS_DATASET", "dbt_dev_marts")

engine = create_engine(f"bigquery://{PROJECT_ID}")

QUERY = text(
    f"""
    SELECT
        customer.customer_city,
        product.product_category_name_english,
        sum(order_items.line_gross_value) as life_revenue
    FROM `{PROJECT_ID}.{MARTS_DATASET}.fct_order_items` AS order_items
    INNER JOIN `{PROJECT_ID}.{MARTS_DATASET}.dim_customer` AS customer
        ON order_items.customer_key = customer.customer_key
    INNER JOIN `{PROJECT_ID}.{MARTS_DATASET}.dim_product` AS product
        ON order_items.product_key = product.product_key group by
        customer.customer_city,
        product.product_category_name_english 
        having product.product_category_name_english in ('auto','electronics','drinks','food','baby')
    """
)


@st.cache_data(ttl="1h")
def load_sales_data() -> pd.DataFrame:
    with engine.connect() as connection:
        return pd.read_sql(QUERY, connection)


st.title("Revenue by city for Top Product Categories")

try:
    sales_data = load_sales_data()
except Exception as exc:
    st.error(f"Failed to query BigQuery: {exc}")
    st.stop()

if sales_data.empty:
    st.warning("No sales records are available to display.")
    st.stop()

df1 = sales_data.head(500).copy()

figure = px.sunburst(
    df1,
    path=["product_category_name_english","customer_city"],
    values="life_revenue",
    title="Top Revenue by product categories and cities",
)

st.plotly_chart(figure, use_container_width=True)
