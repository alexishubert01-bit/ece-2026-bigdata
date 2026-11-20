"""Exercise 2 (principal dev): create a gold namespace with a table, to test silver_reader."""
from pyspark.sql import functions as F

from spark_session import get_spark


def main():
    spark = get_spark("lab-polaris-06-gold")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
    (
        spark.table("polaris.silver.orders")
        .groupBy("product")
        .agg(F.count("*").alias("orders"), F.sum("quantity").alias("quantity"))
        .writeTo("polaris.gold.orders_by_product")
        .createOrReplace()
    )
    spark.sql("SHOW TABLES IN polaris.gold").show()
    spark.sql("SELECT * FROM polaris.gold.orders_by_product ORDER BY product").show()

    spark.stop()


if __name__ == "__main__":
    main()
