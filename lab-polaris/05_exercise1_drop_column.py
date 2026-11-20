"""Exercise 1 (principal dev): drop the country column and show it is a metadata operation."""
from spark_session import get_spark


def counts(spark):
    snapshots = spark.sql("SELECT count(*) FROM polaris.silver.users.snapshots").collect()[0][0]
    files = spark.sql("SELECT count(*) FROM polaris.silver.users.files").collect()[0][0]
    return snapshots, files


def main():
    spark = get_spark("lab-polaris-05-drop-column")

    before = counts(spark)
    print("before: snapshots, data files =", before)

    if "country" in spark.table("polaris.silver.users").columns:
        spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country")

    after = counts(spark)
    print("after:  snapshots, data files =", after)
    print("columns:", spark.table("polaris.silver.users").columns)
    spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()
    spark.sql("""
        SELECT timestamp, file FROM polaris.silver.users.metadata_log_entries
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
