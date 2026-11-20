"""Step 4 (principal dev): the denied INSERT of the analyst must not have created a snapshot."""
from spark_session import get_spark


def main():
    spark = get_spark("lab-polaris-04-check")

    spark.sql("""
        SELECT snapshot_id, operation, committed_at
        FROM polaris.silver.orders.snapshots ORDER BY committed_at
    """).show(truncate=False)
    spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
    spark.sql("DESCRIBE polaris.silver.users").show()

    spark.stop()


if __name__ == "__main__":
    main()
