"""Lab 2, step 6: gold layer built with CREATE TABLE AS SELECT, in the gold namespace."""
from trino_client import show, sql

sql("CREATE SCHEMA IF NOT EXISTS lakehouse.gold")
# CREATE OR REPLACE: the script can be re-run. DROP TABLE fails, Polaris refuses the purge Trino requests
show("schemas", "SHOW SCHEMAS FROM lakehouse")


show("CTAS orders_by_state_month (rows written)", """
    CREATE OR REPLACE TABLE lakehouse.gold.orders_by_state_month AS
    SELECT
        u.state,
        date_trunc('month', o.ordered_at) AS month,
        count(*)                          AS orders,
        sum(o.quantity)                   AS units,
        count(DISTINCT o.user_id)         AS buyers
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY 1, 2
""")
show("orders_by_state_month", """
    SELECT * FROM lakehouse.gold.orders_by_state_month ORDER BY month, state LIMIT 10
""")

show("CTAS top_products (rows written)", """
    CREATE OR REPLACE TABLE lakehouse.gold.top_products AS
    SELECT
        product,
        count(*)      AS orders,
        sum(quantity) AS units,
        rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
    FROM lakehouse.silver.orders
    GROUP BY product
""")
show("top_products", "SELECT * FROM lakehouse.gold.top_products ORDER BY units_rank LIMIT 10")

# LEFT JOIN from the users: the users who never ordered are kept
show("CTAS user_activity (rows written)", """
    CREATE OR REPLACE TABLE lakehouse.gold.user_activity AS
    SELECT
        u.user_id,
        u.state,
        date_diff('year', u.birthdate, current_date) AS age,
        count(o.order_id)            AS orders,
        coalesce(sum(o.quantity), 0) AS units,
        min(o.ordered_at)            AS first_order_at,
        max(o.ordered_at)            AS last_order_at
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY u.user_id, u.state, u.birthdate
""")
show("user_activity", "SELECT * FROM lakehouse.gold.user_activity ORDER BY orders DESC LIMIT 10")

show("gold tables", "SHOW TABLES FROM lakehouse.gold")
show("top_products$files", """
    SELECT file_path, record_count, file_size_in_bytes
    FROM lakehouse.gold."top_products$files"
""")
