"""Exercise 4: compact top_products with optimize, snapshots and files before and after."""
from trino_client import show, sql


def state(label):
    show(f"{label}: snapshots", """
        SELECT snapshot_id, committed_at, operation
        FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at
    """)
    show(f"{label}: files", """
        SELECT file_path, record_count, file_size_in_bytes
        FROM lakehouse.gold."top_products$files"
    """)


state("before")
sql("ALTER TABLE lakehouse.gold.top_products EXECUTE optimize")
state("after")
