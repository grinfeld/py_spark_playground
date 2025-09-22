#!/bin/bash

# init minIO

sleep 10
mc alias set myminio "$STORAGE_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb myminio/"$STORAGE_BUCKET"
mc mb myminio/spark-output
mc mb myminio/spark-checkpoints
mc mb myminio/"$CATALOG_WAREHOUSE_NAME"
mc mb myminio/examples
mc mb myminio/data
mc mb myminio/dbt
mc anonymous set public myminio/"$STORAGE_BUCKET"
mc anonymous set public myminio/spark-output
mc anonymous set public myminio/spark-checkpoints
mc anonymous set public myminio/"$CATALOG_WAREHOUSE_NAME"
mc anonymous set public myminio/data
mc anonymous set public myminio/examples
mc anonymous set public myminio/dbt
mc mirror --overwrite /examples/ myminio/examples/
echo 'MinIO setup completed'