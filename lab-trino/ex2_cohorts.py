"""Exercise 2: gold.cohorts, per decade of birth: users, orders, average units per order."""
from trino_client import show, sql

show("CTAS cohorts (rows written)", """
    CREATE OR REPLACE TABLE lakehouse.gold.cohorts AS
    SELECT
        year(u.birthdate) / 10 * 10       AS birth_decade,
        count(DISTINCT u.user_id)         AS users,
        count(o.order_id)                 AS orders,
        round(avg(o.quantity), 2)         AS avg_units_per_order
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY 1
""")
show("cohorts", "SELECT * FROM lakehouse.gold.cohorts ORDER BY birth_decade")
