#!/bin/bash
# usage: [POLARIS_PRINCIPAL=analyst] ./run.sh script.py
log="run_${1%.py}.log"
setsid nohup spark-submit --driver-memory 1g \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  "$1" > "$log" 2>&1 < /dev/null &
sleep 5
while pgrep -f "SparkSubmit.*$1" > /dev/null; do sleep 3; done
grep -v WARN "$log" | tail -60 | cut -c1-220
