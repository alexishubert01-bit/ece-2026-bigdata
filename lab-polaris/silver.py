"""Silver layer: the transformations of the dbt lab (stg_users, stg_orders) with the DataFrame API.

Same columns and same rules as the dbt models, only the engine changes.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_session import BUCKET


def read_bronze(spark):
    """Read the two bronze CSV files directly from S3."""
    options = dict(header=True, inferSchema=True, samplingRatio=0.01, multiLine=True)
    users_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/users.csv", **options)
    orders_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/orders.csv", **options)
    return users_bronze, orders_bronze


def build_stg_users(users_bronze):
    """Rename and cast the columns, split the address into street, city, state, zip code."""
    users_typed = (
        users_bronze
        .select(
            F.col("uuid").cast("string").alias("user_id"),
            F.trim("username").alias("username"),
            F.trim("name").alias("name"),
            F.upper(F.trim("sex")).alias("sex"),
            F.lower(F.trim("mail")).alias("email"),
            F.col("birthdate").cast("date").alias("birthdate"),
            F.split(F.col("address"), "\n").alias("address_lines"),
        )
        .withColumn("street", F.element_at("address_lines", 1))
        .withColumn("address_line_2", F.element_at("address_lines", 2))
        .drop("address_lines")
    )

    stg_users = users_typed.select(
        "user_id", "username", "name", "sex", "email", "birthdate", "street",
        F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
        F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
        F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
    )

    # Unlike SQL's nullif(), regexp_extract() returns an empty string, not NULL, when there
    # is no match. Left as an empty string, "city" would silently count as a group in a
    # later groupBy() instead of being excluded like a NULL.
    return stg_users.withColumn(
        "city", F.when(F.col("city") == "", None).otherwise(F.col("city"))
    )


def type_orders(orders_bronze):
    """Rename and cast the columns of the orders."""
    return orders_bronze.select(
        F.col("uuid").cast("string").alias("order_id"),
        F.col("user_uuid").cast("string").alias("user_id"),
        F.col("date").cast("timestamp").alias("ordered_at"),
        F.col("quantity").cast("int").alias("quantity"),
        F.lower(F.trim("product")).alias("product"),
    )


def deduplicate_orders(orders):
    """Keep the most recent row per order_id (robust to a replay of the bronze layer).

    partitionBy() is a wide transformation: rows are shuffled so that every row of a given
    order_id lands on the same partition before row_number() can be computed.
    """
    dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
    return (
        orders
        .withColumn("row_number", F.row_number().over(dedup_window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


def build_stg_orders(orders_bronze):
    return deduplicate_orders(type_orders(orders_bronze))
