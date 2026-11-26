"""Exercise 1: partitioned copy of orders_by_state_month, files and input rows compared."""
from trino_client import show, sql

sql("""
    CREATE OR REPLACE TABLE lakehouse.gold.orders_by_state_month_part
    WITH (partitioning = ARRAY['month(month)'])
    AS SELECT * FROM lakehouse.gold.orders_by_state_month
""")

# The partition column of $files only exists for a partitioned table
show("orders_by_state_month$files", """
    SELECT file_path, record_count, file_size_in_bytes
    FROM lakehouse.gold."orders_by_state_month$files"
""")
show("orders_by_state_month_part$files", """
    SELECT partition, record_count, file_size_in_bytes
    FROM lakehouse.gold."orders_by_state_month_part$files"
""")

FILTER = "WHERE month = TIMESTAMP '2020-02-01 00:00:00 UTC'"
for table in ("orders_by_state_month", "orders_by_state_month_part"):
    plan = sql(f"EXPLAIN ANALYZE SELECT sum(units) FROM lakehouse.gold.{table} {FILTER}").iloc[0, 0]
    print(f"\n=== EXPLAIN ANALYZE on {table}, lines with the input rows")
    print("\n".join(line for line in plan.splitlines() if "Input:" in line or "ScanFilter" in line or "TableScan" in line))
