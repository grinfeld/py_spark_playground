#!/bin/bash

set -e

# expects to receive image tag as first argument
docker build -t "py-spark-spark:$1" -f Dockerfile.spark .
docker build --build-arg PY_SPARK_VERSION="$1" -t "py-spark-dbt:$1" -f Dockerfile.dbt .
docker build --build-arg PY_SPARK_VERSION="$1" -t "py-spark-airflow:$1" -f Dockerfile.airflow .
docker build -t "py-spark-fake:$1" -f Dockerfile.fake .

if [[ $1 != "latest" ]]; then
  docker tag "py-spark-spark:$1" py-spark-spark:latest
  docker tag "py-spark-dbt:$1" py-spark-dbt:latest
  docker tag "py-spark-airflow:$1" py-spark-airflow:latest
  docker tag "py-spark-fake:$1" py-spark-airflow:latest
fi