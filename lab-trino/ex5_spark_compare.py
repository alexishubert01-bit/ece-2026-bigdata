"""Exercise 5 (Spark, Polaris catalog): orders_by_state_month computed by Spark through Polaris.

Run with ./run_ex5.sh (PySpark 4.1.1, see the script). The session comes from lab-polaris/spark_session.py.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lab-polaris"))
from spark_session import get_spark  # noqa: E402


def main():
    spark = get_spark("lab-trino-ex5")
    # Same month boundaries as Trino, which truncates in UTC
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark_result = spark.sql("""
        SELECT u.state,
               date_trunc('month', o.ordered_at) AS month,
               count(*)                          AS orders,
               sum(o.quantity)                   AS units,
               count(DISTINCT o.user_id)         AS buyers
        FROM polaris.silver.orders AS o
        JOIN polaris.silver.users  AS u ON u.user_id = o.user_id
        GROUP BY 1, 2
    """)
    print("Spark rows      :", spark_result.count())
    print("Spark units     :", spark_result.agg({"units": "sum"}).first()[0])
    # The gold table written by Trino, read back by Spark through the same catalog
    trino_table = spark.table("polaris.gold.orders_by_state_month")
    print("Trino gold rows :", trino_table.count())
    print("Trino gold units:", trino_table.agg({"units": "sum"}).first()[0])
    spark.stop()


if __name__ == "__main__":
    main()
