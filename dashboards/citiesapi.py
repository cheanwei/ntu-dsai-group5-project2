import os
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify
from sqlalchemy import create_engine, text

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

PROJECT_ID = os.getenv("GCP_PROJECT", "dsai6mod2")
MARTS_DATASET = os.getenv("BIGQUERY_MARTS_DATASET", "dbt_dev_marts")
TOP_CITIES_LIMIT = int(os.getenv("TOP_CITIES_LIMIT", "10"))

engine = create_engine(f"bigquery://{PROJECT_ID}")
app = Flask(__name__)


@app.get("/api/cities")
def get_cities():
    query = text(
        f"""
        WITH ranked_customers AS (
            SELECT
                customer_key,
                customer_city,
                customer_latitude,
                customer_longitude,
                lifetime_revenue,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_city
                    ORDER BY lifetime_revenue DESC, customer_key
                ) AS revenue_rank
            FROM `{PROJECT_ID}.{MARTS_DATASET}.dim_customer`
            WHERE customer_city IS NOT NULL
              AND customer_latitude IS NOT NULL
              AND customer_longitude IS NOT NULL
        ),
        city_revenue AS (
            SELECT
                customer_city,
                AVG(customer_latitude) AS latitude,
                AVG(customer_longitude) AS longitude,
                COUNT(*) AS customer_count,
                SUM(lifetime_revenue) AS total_lifetime_revenue
            FROM ranked_customers
            GROUP BY customer_city
        )
        SELECT
            city.customer_city AS name,
            city.latitude,
            city.longitude,
            city.customer_count,
            city.total_lifetime_revenue,
            customer.customer_key AS top_customer_key,
            customer.lifetime_revenue AS top_customer_lifetime_revenue
        FROM city_revenue AS city
        JOIN ranked_customers AS customer
          ON city.customer_city = customer.customer_city
         AND customer.revenue_rank = 1
        ORDER BY city.total_lifetime_revenue DESC
        LIMIT {TOP_CITIES_LIMIT}
        """
    )

    with engine.connect() as connection:
        rows = connection.execute(query).mappings()
        cities = [
            {
                "name": row["name"],
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "customer_count": int(row["customer_count"]),
                "total_lifetime_revenue": float(row["total_lifetime_revenue"]),
                "top_customer_key": row["top_customer_key"],
                "top_customer_lifetime_revenue": float(
                    row["top_customer_lifetime_revenue"]
                ),
            }
            for row in rows
        ]

    return jsonify({"cities": cities})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
