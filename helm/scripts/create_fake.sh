#!/bin/bash

NAMESPACE=$1
TAG=$2

helm create "fake"

if [ -z "$KAFKA_BOOTSTRAP" ]; then
    exit
  fi
echo "!!!!!!!!! $KAFKA_BOOTSTRAP"
# NEW (portable and safe)
FAKE_ENVS=$(cat <<EOF
env:
  PORT: 8090
  KAFKA_BROKERS: $KAFKA_BOOTSTRAP
  KAFKA_TOPIC: source-topic
  SPARK_MASTER_URL: "k8s://https://kubernetes.default.svc:443"
  STORAGE_BUCKET: spark-data
  CATALOG_TYPE: hadoop
  CATALOG_WAREHOUSE_NAME: mycatalog
  CATALOG_WAREHOUSE_PATH: s3a://mycatalog
  STORAGE_ENDPOINT: http://minio:9000
  STORAGE_ENDPOINT_SSL_ENABLE: "false"
  STORAGE_ACCESS_KEY_ID: minioadmin
  STORAGE_SECRET_KEY: minioadmin
  STORAGE_CREDENTIALS_PROVIDER: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
  STORAGE_PATH_STYLE_ACCESS: "true"
  STORAGE_SSL_ENABLED: "false"
  AWS_REGION: us-east-1
  CATALOG_IO_IMPL: org.apache.iceberg.hadoop.HadoopFileIO
  CATALOG_IMPL: org.apache.iceberg.spark.SparkCatalog
  SPARK_EXECUTOR_IMAGE: py-spark-spark:$TAG
  SPARK_NAMESPACE: py-spark
  SPARK_SERVICE_ACCOUNT: fake
  SPARK_DRIVER_HOST:
EOF
)

source "$HELM_BASE_DIR/scripts/create_deployment.sh" "-n=$NAMESPACE" "-s=fake" "-i=py-spark-fake" "-t=$TAG" "-p=8090" "-pf=8090" "$FAKE_ENVS"