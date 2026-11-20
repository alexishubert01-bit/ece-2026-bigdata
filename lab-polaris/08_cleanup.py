"""Cleanup (principal dev): drop the tables created in Polaris, keep silver.orders."""
from spark_session import get_spark


def main():
    spark = get_spark("lab-polaris-08-cleanup")

    spark.sql("DROP TABLE IF EXISTS polaris.gold.orders_by_product")
    spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
    spark.sql("DROP TABLE IF EXISTS polaris.silver.users")

    spark.sql("SHOW NAMESPACES IN polaris").show()
    spark.sql("SHOW TABLES IN polaris.silver").show()

    spark.stop()


if __name__ == "__main__":
    main()
