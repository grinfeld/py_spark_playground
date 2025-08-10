#!/bin/bash

# init minIO

sleep 10
mc alias set myminio "$STORAGE_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb myminio/"$STORAGE_BUCKET"
mc mb myminio/spark-output
mc mb myminio/spark-checkpoints
mc anonymous set public myminio/"$STORAGE_BUCKET"
mc anonymous set public myminio/spark-output
mc anonymous set public myminio/spark-checkpoints
echo 'MinIO setup completed'