"""Exercise 2 (principal reader): silver_reader reads silver but must not read gold."""
from spark_session import get_spark


def check(spark, label, sql):
    try:
        rows = spark.sql(sql).collect()
        print(f"{label}: OK {rows[:2]}")
    except Exception as exc:
        reason = next((l for l in str(exc).splitlines() if "Exception" in l), str(exc)[:300])
        print(f"{label}: DENIED -> {reason[:300]}")


def main():
    spark = get_spark("lab-polaris-07-reader")

    check(spark, "SHOW NAMESPACES (catalog level)", "SHOW NAMESPACES IN polaris")
    check(spark, "SHOW TABLES in silver", "SHOW TABLES IN polaris.silver")
    check(spark, "READ silver.orders", "SELECT count(*) AS orders FROM polaris.silver.orders")
    check(spark, "READ silver.users", "SELECT count(*) AS users FROM polaris.silver.users")
    check(spark, "SHOW TABLES in gold", "SHOW TABLES IN polaris.gold")
    check(spark, "READ gold.orders_by_product", "SELECT * FROM polaris.gold.orders_by_product")

    spark.stop()


if __name__ == "__main__":
    main()
