"""Lab 2, steps 1 to 3: connection, catalogs, silver tables, consistency checks."""
from trino_client import show

show("version", "SELECT version()")

# Catalogs, schemas and tables
show("catalogs", "SHOW CATALOGS")
show("schemas of lakehouse", "SHOW SCHEMAS FROM lakehouse")
show("tables of lakehouse.silver", "SHOW TABLES FROM lakehouse.silver")
show("describe silver.orders", "DESCRIBE lakehouse.silver.orders")
show("describe silver.users", "DESCRIBE lakehouse.silver.users")

# Query the silver layer
show("orders", "SELECT count(*) AS orders FROM lakehouse.silver.orders")
show("users", "SELECT count(*) AS users FROM lakehouse.silver.users")
show("address parsing", """
    SELECT state,
           count(*) AS users,
           count_if(city IS NULL) AS missing_city
    FROM lakehouse.silver.users
    GROUP BY state
    ORDER BY users DESC
    LIMIT 10
""")
show("orphan orders", """
    SELECT count(*) AS orphan_orders
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
    WHERE u.user_id IS NULL
""")
