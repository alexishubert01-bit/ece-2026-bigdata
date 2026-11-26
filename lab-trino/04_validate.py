"""Lab 2, step 7: validate the gold layer against the silver layer. No output = every assertion passed."""
from trino_client import value

silver_units = value("SELECT sum(quantity) FROM lakehouse.silver.orders")
gold_units = value("SELECT sum(units) FROM lakehouse.gold.orders_by_state_month")
product_units = value("SELECT sum(units) FROM lakehouse.gold.top_products")
assert silver_units == gold_units, f"{silver_units} != {gold_units}"
assert silver_units == product_units, f"{silver_units} != {product_units}"

silver_users = value("SELECT count(*) FROM lakehouse.silver.users")
gold_users = value("SELECT count(*) FROM lakehouse.gold.user_activity")
silver_orders = value("SELECT count(*) FROM lakehouse.silver.orders")
gold_orders = value("SELECT sum(orders) FROM lakehouse.gold.user_activity")
assert silver_users == gold_users, f"{silver_users} != {gold_users}"
assert silver_orders == gold_orders, f"{silver_orders} != {gold_orders}"

print(f"OK: units {silver_units}, users {silver_users}, orders {silver_orders}")
