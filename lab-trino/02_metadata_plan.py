"""Lab 2, steps 4 and 5: Iceberg metadata tables, query plan and execution statistics."""
from trino_client import show, sql

show("orders$snapshots", """
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.silver."orders$snapshots"
    ORDER BY committed_at
""")
show("orders$files", """
    SELECT file_path, file_format, record_count, file_size_in_bytes
    FROM lakehouse.silver."orders$files"
""")
show("orders$history", 'SELECT * FROM lakehouse.silver."orders$history"')
show("orders$manifests", """
    SELECT path, added_data_files_count, existing_data_files_count
    FROM lakehouse.silver."orders$manifests"
""")

JOIN_QUERY = """
    SELECT u.state, count(*) AS orders
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY u.state
"""

print("\n=== EXPLAIN (TYPE DISTRIBUTED)")
print(sql(f"EXPLAIN (TYPE DISTRIBUTED) {JOIN_QUERY}").iloc[0, 0])

print("\n=== EXPLAIN ANALYZE")
print(sql(f"EXPLAIN ANALYZE {JOIN_QUERY}").iloc[0, 0])

try:
    show("last queries", """
        SELECT query_id, state,
               date_diff('millisecond', created, coalesce("end", current_timestamp)) AS elapsed_ms,
               substr(query, 1, 60) AS query
        FROM system.runtime.queries
        ORDER BY created DESC
        LIMIT 5
    """)
except Exception as error:  # access control may restrict the system catalog
    print("system.runtime.queries denied:", error)
