"""Lab 3: validate the star against the silver layer. The assertions of the statement, then extra checks."""
from trino_client import show, value

silver_orders = value("SELECT count(*) FROM lakehouse.silver.orders")
fact_orders = value("SELECT count(*) FROM lakehouse.gold.fact_orders")
silver_units = value("SELECT sum(quantity) FROM lakehouse.silver.orders")
fact_units = value("SELECT sum(quantity) FROM lakehouse.gold.fact_orders")

# No order is lost or duplicated by the lookups of the keys
assert silver_orders == fact_orders, f"{silver_orders} != {fact_orders}"
assert silver_units == fact_units, f"{silver_units} != {fact_units}"

# Every order finds its three dimension rows
unmatched = value("""
    SELECT count(*)
    FROM lakehouse.gold.fact_orders AS f
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    LEFT JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
    WHERE u.user_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL
""")
assert unmatched == 0, f"{unmatched} orders without a complete dimension row"

# Additional checks (answer to the first question): reported, not asserted, to show what they catch
dup_orders = value("SELECT count(*) - count(DISTINCT order_id) FROM lakehouse.gold.fact_orders")
dup_users = value("SELECT count(*) - count(DISTINCT user_id) FROM lakehouse.gold.dim_user")
mismatch = value("""
    SELECT count(*)
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.silver.orders AS o ON o.order_id = f.order_id
    JOIN lakehouse.gold.dim_user AS u ON u.user_key = f.user_key
    JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    WHERE u.user_id <> o.user_id OR p.product <> o.product OR f.quantity <> o.quantity
""")
print(f"OK (statement checks): {fact_orders} orders, {fact_units} units, every key found")
print(f"extra check, duplicated order_id in fact_orders: {dup_orders}")
print(f"extra check, duplicated user_id in dim_user: {dup_users}")
print(f"extra check, rows linked to the wrong user, product or quantity: {mismatch}")
show("duplicated order_id in silver.orders", """
    SELECT order_id, count(*) AS n, array_agg(source) AS sources
    FROM lakehouse.silver.orders
    GROUP BY order_id
    HAVING count(*) > 1
""")
