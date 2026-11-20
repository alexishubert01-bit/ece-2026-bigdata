"""Step 3 (principal analyst): reads are allowed, writes and schema changes must be denied."""
from spark_session import get_spark


def try_sql(spark, label, sql):
    try:
        spark.sql(sql)
        print(f"{label}: SUCCEEDED (unexpected)")
    except Exception as exc:
        print(f"{label}: FAILED with {type(exc).__name__}")
        print(str(exc)[:500])


def main():
    spark = get_spark("lab-polaris-03-analyst")

    spark.sql("SHOW TABLES IN polaris.silver").show()
    spark.sql("""
        SELECT product, count(*) AS orders FROM polaris.silver.orders GROUP BY product
    """).show()

    try_sql(spark, "INSERT", """
        INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1
    """)
    try_sql(spark, "ALTER", "ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")

    spark.stop()


if __name__ == "__main__":
    main()
