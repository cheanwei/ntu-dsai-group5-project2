{% docs star_schema_design %}

The marts use a fact constellation: four facts retain their natural grains and
share customer, product, seller, and date dimensions.

- Use `fct_orders.payment_value_total` for order-level revenue.
- Use `fct_order_items.line_gross_value` for product and seller analysis.
- Do not join payments directly to order items; aggregate at order grain first.
- Use `customer_key`, derived from `customer_unique_id`, for customer analysis.

| Model | Grain |
|---|---|
| `fct_orders` | one order |
| `fct_order_items` | `order_id + order_item_id` |
| `fct_payments` | `order_id + payment_sequential` |
| `fct_reviews` | one deduplicated review |
| `dim_customer` | `customer_unique_id` |
| `dim_product` | `product_id` |
| `dim_seller` | `seller_id` |
| `dim_date` | one calendar date |

{% enddocs %}

{% docs dim_customer_overview %}

Customer dimension keyed by the stable `customer_unique_id`. It contains the
latest observed location, order counts, repeat status, lifetime revenue, and
recency.

{% enddocs %}

{% docs fct_orders_overview %}

Order-grain accumulating snapshot with lifecycle timestamps, delivery metrics,
and item/payment totals reconciled at order grain. Use
`payment_value_total` for order-level revenue.

{% enddocs %}

{% docs fct_order_items_overview %}

Atomic sales fact at one row per order line. Use it for product, seller, price,
freight, and line gross value; do not sum payments at this grain.

{% enddocs %}

{% docs fct_payments_overview %}

One row per payment sequence within an order. Use it for payment-method and
installment analysis.

{% enddocs %}

{% docs fct_reviews_overview %}

One row per deduplicated review, including score, comments, response time, and
review date. Join to `fct_orders` on `order_id`.

{% enddocs %}

{% docs dim_date_overview %}

Calendar dimension from 2016-09-01 through 2018-12-31. The
`is_complete_month` flag identifies periods safe for trend comparisons.

{% enddocs %}

{% docs dim_product_overview %}

Product dimension with Portuguese and English categories plus physical
attributes. Missing categories are represented as `unknown`.

{% enddocs %}

{% docs dim_seller_overview %}

Seller dimension with city, state, zip prefix, and median zip-level
coordinates.

{% enddocs %}
