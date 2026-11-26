#!/bin/bash
# Exercise 5 with PySpark 4.1.1: the service image ships Spark 4.3.0, for which no Iceberg runtime exists
# (iceberg-spark-runtime stops at Spark 4.1). The S3A jars of the image are put on the driver classpath.
set -e
J=/opt/spark/jars
CP=$(ls $J/hadoop-client-api-*.jar $J/hadoop-client-runtime-*.jar $J/hadoop-aws-*.jar $J/bundle-*.jar $J/*analytics*.jar | paste -sd:)
env -u SPARK_HOME -u PYTHONPATH \
  PYSPARK_SUBMIT_ARGS="--driver-memory 512m --driver-class-path $CP pyspark-shell" \
  POLARIS_PRINCIPAL="${POLARIS_PRINCIPAL:-root}" POLARIS_ROOT_CREDENTIAL="${POLARIS_ROOT_CREDENTIAL:-root:s3cr3t}" \
  uv run -q --no-project --python 3.12 --with 'pyspark==4.1.1' python ex5_spark_compare.py
