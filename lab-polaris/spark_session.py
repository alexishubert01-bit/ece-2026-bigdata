"""SparkSession with S3A, the Hadoop catalog "lab" and the Polaris REST catalog "polaris"."""
import os

from pyspark.sql import SparkSession

BUCKET = os.environ["LAB_BUCKET_NAME"]
POLARIS_URI = os.environ["POLARIS_URI"]
POLARIS_CATALOG = os.environ["POLARIS_CATALOG"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def get_spark(app_name="lab-polaris"):
    # The principal whose credential is used by the "polaris" catalog: dev or analyst
    principal = os.environ.get("POLARIS_PRINCIPAL", "dev")
    credential = os.environ[f"POLARIS_{principal.upper()}_CREDENTIAL"]
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        # S3 access, identical to the Spark lab
        .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION", "us-east-1"))
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Iceberg
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Hadoop catalog of the Spark lab
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hadoop")
        .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
        # Polaris catalog, REST API, principal chosen by POLARIS_PRINCIPAL
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", f"{POLARIS_URI}/api/catalog")
        .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG)
        .config("spark.sql.catalog.polaris.credential", credential)
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        # No credential vending on this storage: files are read with the S3A keys above
        .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("Polaris principal:", principal)
    return spark
