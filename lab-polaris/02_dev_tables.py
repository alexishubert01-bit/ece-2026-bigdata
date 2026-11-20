"""Step 2 (principal dev): create a table through Polaris, then change its schema."""
from spark_session import get_spark


def main():
    spark = get_spark("lab-polaris-02-dev")

    spark.sql("SHOW TABLES IN polaris.silver").show()
    spark.sql("""
        SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots
    """).show(truncate=False)

    # A table created through Polaris: Polaris decides where its files go
    stg_users = spark.table("lab.silver.users")
    stg_users.writeTo("polaris.silver.users").createOrReplace()
    spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()

    # Schema change: a metadata operation
    if "country" not in spark.table("polaris.silver.users").columns:
        spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
    spark.sql("DESCRIBE polaris.silver.users").show()
    spark.sql("""
        SELECT timestamp, file, latest_schema_id
        FROM polaris.silver.users.metadata_log_entries
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
