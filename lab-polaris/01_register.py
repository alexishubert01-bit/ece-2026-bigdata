"""Step 1: compare the two catalogs, then register lab.silver.orders in Polaris."""
import os

from spark_session import BUCKET, get_spark


def main():
    version = os.environ["ORDERS_METADATA_VERSION"]
    spark = get_spark("lab-polaris-01-register")

    # Which component answers? "lab" is a directory of the bucket, "polaris" a REST service
    spark.sql("SHOW NAMESPACES IN lab").show()
    spark.sql("SHOW NAMESPACES IN polaris").show()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")

    # Registration only adds a pointer to the current metadata file, no data is copied
    tables = [r.tableName for r in spark.sql("SHOW TABLES IN polaris.silver").collect()]
    if "orders" not in tables:
        spark.sql(f"""
            CALL polaris.system.register_table(
              table => 'silver.orders',
              metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v{version}.metadata.json'
            )
        """).show(truncate=False)

    spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
    spark.sql("SHOW TABLES IN polaris.silver").show()

    spark.stop()


if __name__ == "__main__":
    main()
