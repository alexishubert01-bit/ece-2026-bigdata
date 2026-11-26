"""Exercise 3: append one order to silver, refresh orders_by_state_month with MERGE, time travel."""
from trino_client import show, sql, value

before = value("""
    SELECT snapshot_id FROM lakehouse.gold."orders_by_state_month$snapshots"
    ORDER BY committed_at DESC LIMIT 1
""")

# One new order, for an existing user, in a month that already exists for that user's state
sql("""
    INSERT INTO lakehouse.silver.orders (order_id, user_id, ordered_at, quantity, product)
    SELECT 'ex3-' || CAST(uuid() AS varchar), user_id,
           TIMESTAMP '2020-02-15 10:00:00 UTC', 4, 'croissant'
    FROM lakehouse.silver.users
    ORDER BY user_id
    LIMIT 1
""")

sql("""
    MERGE INTO lakehouse.gold.orders_by_state_month AS t
    USING (
        SELECT u.state,
               date_trunc('month', o.ordered_at) AS month,
               count(*)                          AS orders,
               sum(o.quantity)                   AS units,
               count(DISTINCT o.user_id)         AS buyers
        FROM lakehouse.silver.orders AS o
        JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
        GROUP BY 1, 2
    ) AS s
    ON t.state = s.state AND t.month = s.month
    WHEN MATCHED AND (t.orders <> s.orders OR t.units <> s.units OR t.buyers <> s.buyers)
        THEN UPDATE SET orders = s.orders, units = s.units, buyers = s.buyers
    WHEN NOT MATCHED
        THEN INSERT (state, month, orders, units, buyers)
             VALUES (s.state, s.month, s.orders, s.units, s.buyers)
""")

show("orders_by_state_month$snapshots", """
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold."orders_by_state_month$snapshots"
    ORDER BY committed_at
""")
show("units now vs before the MERGE", f"""
    SELECT
        (SELECT sum(units) FROM lakehouse.gold.orders_by_state_month) AS units_now,
        (SELECT sum(units) FROM lakehouse.gold.orders_by_state_month FOR VERSION AS OF {before}) AS units_before,
        (SELECT sum(quantity) FROM lakehouse.silver.orders) AS silver_units
""")
