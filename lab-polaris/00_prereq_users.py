"""Step 0: create lab.silver.users, expected by the Polaris lab (it comes from the Spark lab)."""
from silver import build_stg_users, read_bronze
from spark_session import get_spark


def main():
    spark = get_spark("lab-polaris-00-users")

    users_bronze, _ = read_bronze(spark)
    build_stg_users(users_bronze).writeTo("lab.silver.users").createOrReplace()

    spark.sql("SELECT count(*) AS users FROM lab.silver.users").show()
    spark.sql("SHOW TABLES IN lab.silver").show()

    spark.stop()


if __name__ == "__main__":
    main()
