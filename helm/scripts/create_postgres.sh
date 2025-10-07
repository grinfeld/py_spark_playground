#!/bin/bash

NAMESPACE=$1

helm create postgres

source "$HELM_BASE_DIR/scripts/define_service.sh" "postgres"

source "$HELM_BASE_DIR/scripts/define_statefulset.sh" "postgres"

read -r -d '' OVERRIDE_VALUES << EOF
nameOverride: postgres
fullnameOverride: postgres

image:
  repository: postgres
  tag: 13
  pullPolicy: IfNotPresent

containers:
  ports:
    - 5432

service:
  type: ClusterIP
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432

persistence:
  size: 1Gi
  mountPath: /var/lib/postgresql/data

env:
  POSTGRES_USER: airflow
  POSTGRES_PASSWORD: airflow
  POSTGRES_DB: airflow
EOF

python update_values.py -r postgres -p "$HELM_BASE_DIR/postgres/values.yaml" -v "$OVERRIDE_VALUES"

helm upgrade --install postgres-release ./postgres --namespace "$NAMESPACE"