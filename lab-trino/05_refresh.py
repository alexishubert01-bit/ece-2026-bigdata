"""Lab 2, step 8: refresh top_products with DELETE + INSERT, then time travel to its first snapshot."""
from trino_client import show, sql, value

sql("DELETE FROM lakehouse.gold.top_products")
sql("""
    INSERT INTO lakehouse.gold.top_products
    SELECT product, count(*), sum(quantity),
           rank() OVER (ORDER BY sum(quantity) DESC)
    FROM lakehouse.silver.orders
    GROUP BY product
""")

show("top_products$snapshots", """
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold."top_products$snapshots"
    ORDER BY committed_at
""")

first = value("""
    SELECT snapshot_id
    FROM lakehouse.gold."top_products$snapshots"
    ORDER BY committed_at
    LIMIT 1
""")
show(f"products as of the first snapshot {first}",
     f"SELECT count(*) AS products FROM lakehouse.gold.top_products FOR VERSION AS OF {first}")
show("products now", "SELECT count(*) AS products FROM lakehouse.gold.top_products")
