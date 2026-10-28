"""Step 1: SparkSession with S3 access, then read the bronze CSV datasets from S3.

Run from this folder:
    spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 01_bronze.py
"""
from spark_session import BUCKET, get_spark


def main():
    spark = get_spark("lab-silver-01-bronze")

    # inferSchema triggers an extra pass over the file: acceptable on tiny datasets only,
    # the schema should be defined explicitly otherwise.
    # multiLine=True because each address spans two physical lines.
    users_bronze = spark.read.csv(
        f"s3a://{BUCKET}/bronze/users.csv",
        header=True, inferSchema=True, samplingRatio=0.01, multiLine=True,
    )
    orders_bronze = spark.read.csv(
        f"s3a://{BUCKET}/bronze/orders.csv",
        header=True, inferSchema=True, samplingRatio=0.01, multiLine=True,
    )

    users_bronze.printSchema()
    users_bronze.show(3, truncate=False)

    # The address column spans 2 lines: the street, then the city, the state and the zip code
    users_bronze.select("address").show(3, truncate=False)

    orders_bronze.printSchema()
    print("users rows :", users_bronze.count())
    print("orders rows:", orders_bronze.count())

    spark.stop()


if __name__ == "__main__":
    main()
