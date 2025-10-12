#!/bin/bash

NAMESPACE=$1

helm create minio

source "$HELM_BASE_DIR/scripts/define_service.sh" "minio"

source "$HELM_BASE_DIR/scripts/define_statefulset.sh" "minio"

source "$HELM_BASE_DIR/scripts/define_post_deploy.sh" "minio"

CURRENT_PATH=$(pwd)
cd ../
EXAMPLE_PATH="$(pwd)/data"
SPARK_CODE_PATH="$(pwd)/dags/spark/"
cd "$CURRENT_PATH"
TEMPLATE_PATH="$HELM_BASE_DIR/templates/"

read -r -d '' OVERRIDE_VALUES << EOF
nameOverride: minio
fullnameOverride: minio

persistence:
  size: 8Gi
  mountPath: /data

containers:
  ports:
    - 9000
    - 9001

env:
  MINIO_ROOT_USER: minioadmin
  MINIO_ROOT_PASSWORD: minioadmin
  STORAGE_ENDPOINT: http://minio:9000
  STORAGE_BUCKET: spark-data
  CATALOG_WAREHOUSE_NAME: mycatalog

image:
  repository: minio/minio
  pullPolicy: IfNotPresent
  tag: "latest"
  command: ["minio", "server"]
  args: ["/data", "--console-address", ":9001"]

livenessProbe:
    httpGet:
      path: /minio/health/live
      port: 9000
    initialDelaySeconds: 5
    periodSeconds: 5

service:
  type: ClusterIP
  ports:
    - name: api
      port: 9000
      targetPort: 9000
    - name: console
      port: 9001
      targetPort: 9001

post:
  deploy:
    image:
      repository: minio/minio
      tag: "latest"
    hook:
      delete:
        policy: "hook-succeeded,hook-failed"
    hostPathVolume:
      hostVolumes:
        - name: examples
          path: "$EXAMPLE_PATH"
          mountPath: "/examples"
        - name: spark-code
          path: "$SPARK_CODE_PATH"
          mountPath: "/spark/code"
        - name: spark-templates
          path: "$TEMPLATE_PATH"
          mountPath: "/spark/templates"
    script: |
      #!/bin/bash
      sleep 5

      set_minio_alias() {
        # init minIO
        mc alias set myminio "\$STORAGE_ENDPOINT" "\$MINIO_ROOT_USER" "\$MINIO_ROOT_PASSWORD"
        mc admin info myminio
      }

      # Waiting for MinIO to be ready
      echo "Waiting for MinIO to become available"
      until set_minio_alias > /dev/null 2>&1; do
        echo -n "."
        sleep 2
      done
      echo "MinIO is ready!"

      mc mb myminio/"\$STORAGE_BUCKET"
      mc mb myminio/"\$CATALOG_WAREHOUSE_NAME"
      mc mb myminio/spark
      mc mb myminio/examples
      mc mb myminio/data
      mc mb myminio/dbt
      mc anonymous set public myminio/"\$STORAGE_BUCKET"
      mc anonymous set public myminio/"\$CATALOG_WAREHOUSE_NAME"
      mc anonymous set public myminio/spark
      mc anonymous set public myminio/data
      mc anonymous set public myminio/examples
      mc anonymous set public myminio/dbt

      mc put /examples/customers-100000.csv myminio/examples/customers-100000.csv
      mc put /spark/templates/spark_operator_spec.json myminio/spark/templates/spark_operator_spec.json
      mc put /spark/templates/sspark_k8s_template.yaml myminio/spark/templates/sspark_k8s_template.yaml
      mc put /spark/templates/dbt_template.yaml myminio/spark/templates/dbt_template.yaml
      echo 'MinIO setup completed'
EOF

python update_values.py -r minio -p "$HELM_BASE_DIR/minio/values.yaml" -v "$OVERRIDE_VALUES"

set +e
kubectl -n "$NAMESPACE" delete jobs minio-post-deploy-job
set -e

helm upgrade --install minio-release ./minio --namespace "$NAMESPACE" --wait

export MINIO_POD_NAME=$(kubectl get pods --namespace py-spark -l "app.kubernetes.io/name=minio,app.kubernetes.io/instance=minio-release" -o jsonpath="{.items[0].metadata.name}"|  head -n 1)
kubectl --namespace "$NAMESPACE" port-forward "$MINIO_POD_NAME" 9001:9001 >/dev/null 2>&1 &
PID=$!
echo "$MINIO_POD_NAME port-forward PID is $PID"