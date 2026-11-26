"""Lab 3: gold layer as a star schema, dimensions first, then the fact table."""
from trino_client import show, sql


# CREATE OR REPLACE everywhere: DROP TABLE fails, Polaris refuses the purge requested by Trino

# Dimensions with a surrogate key generated in the gold layer
sql("""
    CREATE OR REPLACE TABLE lakehouse.gold.dim_user AS
    SELECT
        row_number() OVER (ORDER BY user_id) AS user_key,
        user_id, username, name, sex, birthdate, city, state, zip_code
    FROM lakehouse.silver.users
""")
sql("""
    CREATE OR REPLACE TABLE lakehouse.gold.dim_product AS
    SELECT row_number() OVER (ORDER BY product) AS product_key, product
    FROM (SELECT DISTINCT product FROM lakehouse.silver.orders)
""")

# Date dimension generated, one row per day between the first and the last order
first_day, last_day = sql("""
    SELECT CAST(min(ordered_at) AS date) AS first_day,
           CAST(max(ordered_at) AS date) AS last_day
    FROM lakehouse.silver.orders
""").iloc[0]
print("date range:", first_day, "to", last_day)

sql(f"""
    CREATE OR REPLACE TABLE lakehouse.gold.dim_date AS
    SELECT
        CAST(date_format(CAST(d AS timestamp), '%Y%m%d') AS integer) AS date_key,
        d                                       AS full_date,
        year(d)                                 AS year,
        quarter(d)                              AS quarter,
        month(d)                                AS month,
        date_format(CAST(d AS timestamp), '%M') AS month_name,
        day_of_week(d)                          AS day_of_week,
        day_of_week(d) IN (6, 7)                AS is_weekend
    FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL '1' DAY)) AS t(d)
""")

# Fact table at the grain of silver.orders: one row per order
sql("""
    CREATE OR REPLACE TABLE lakehouse.gold.fact_orders AS
    SELECT
        o.order_id,
        u.user_key,
        p.product_key,
        CAST(date_format(o.ordered_at, '%Y%m%d') AS integer) AS date_key,
        o.ordered_at,
        o.quantity
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_id = o.user_id
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product = o.product
""")

show("row counts", """
    SELECT 'dim_user' AS table_name, count(*) AS row_count FROM lakehouse.gold.dim_user
    UNION ALL SELECT 'dim_product', count(*) FROM lakehouse.gold.dim_product
    UNION ALL SELECT 'dim_date', count(*) FROM lakehouse.gold.dim_date
    UNION ALL SELECT 'fact_orders', count(*) FROM lakehouse.gold.fact_orders
""")

# Query the star: only the dimensions the question needs
show("units by year, state and product", """
    SELECT d.year, u.state, p.product,
           count(*)        AS orders,
           sum(f.quantity) AS units
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
    JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
    JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    GROUP BY d.year, u.state, p.product
    ORDER BY units DESC
    LIMIT 10
""")
show("orders_by_state_month rebuilt from the star", """
    SELECT u.state, d.year, d.month,
           count(*)                   AS orders,
           sum(f.quantity)            AS units,
           count(DISTINCT f.user_key) AS buyers
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date    AS d ON d.date_key = f.date_key
    JOIN lakehouse.gold.dim_user    AS u ON u.user_key = f.user_key
    GROUP BY u.state, d.year, d.month
    ORDER BY d.year, d.month, u.state
    LIMIT 10
""")
show("orders_by_state_month, first rows to compare", """
    SELECT state, month, orders, units, buyers
    FROM lakehouse.gold.orders_by_state_month
    ORDER BY month, state
    LIMIT 10
""")
