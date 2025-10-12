#!/bin/bash

NAMESPACE=$1

mkdir -p "$HELM_BASE_DIR/strimzi-kafka/"

cat > "$HELM_BASE_DIR/strimzi-kafka/operator-values.yaml" << EOF
watchNamespaces:
  - $NAMESPACE
EOF

helm upgrade --install strimzi-kafka-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --namespace "$NAMESPACE" \
  --values "$HELM_BASE_DIR/strimzi-kafka/operator-values.yaml" \
  --wait

# Create Kafka cluster with external access
cat > "$HELM_BASE_DIR/strimzi-kafka/kafka-cluster.yaml" << EOF
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaNodePool
metadata:
  name: broker-controller
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  replicas: 1
  roles:
    - controller
    - broker
  storage:
    type: jbod
    volumes:
      - id: 0
        type: persistent-claim
        size: 10Gi
        deleteClaim: false
        kraftMetadata: shared
---
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: kafka-cluster
spec:
  kafka:
    version: 4.1.0
    metadataVersion: 4.1-IV1
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
      - name: tls
        port: 9093
        type: internal
        tls: true
      - name: external
        port: 9094
        type: nodeport
        tls: false
        configuration:
          bootstrap:
            nodePort: 32100
    config:
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
      default.replication.factor: 1
      min.insync.replicas: 1
  entityOperator:
    topicOperator: {}
    userOperator: {}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: source-topic
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  partitions: 1
  replicas: 1
  config:
    retention.ms: 604800000 # Example: 7 days retention
EOF

set -e
kubectl delete all,configmap,rolebinding,role,serviceaccount -l app.kubernetes.io/name=strimzi -n py-spark
set +e

# Install operator and wait until it's ready
kubectl wait --for=condition=ready pod -l name=strimzi-cluster-operator -n "$NAMESPACE" --timeout=300s

# Apply the Kafka cluster (not before operator is ready)
kubectl apply -f "$HELM_BASE_DIR/strimzi-kafka/kafka-cluster.yaml" -n "$NAMESPACE"

# Wait for Kafka cluster to be ready
kubectl wait kafka/kafka-cluster --for=condition=Ready --timeout=300s -n "$NAMESPACE"

echo "bootstrap-servers: 'kafka-cluster-kafka-bootstrap.py-spark.svc.cluster.local:9092'"

# helm uninstall strimzi-kafka-operator -n py-spark 2>/dev/null || true
#
## Delete all Strimzi CRDs and resources
# kubectl delete kafka kafka-cluster -n py-spark 2>/dev/null || true
# kubectl delete kafkanodepool dual-role -n py-spark 2>/dev/null || true
#
## Delete RoleBindings, Roles, ServiceAccounts
# kubectl delete rolebinding,role,serviceaccount -l app.kubernetes.io/name=strimzi -n py-spark 2>/dev/null || true
# kubectl -n py-spark delete pod kafka-cluster-broker-controller-0
# kubectl delete service -n py-spark kafka-cluster-broker-controller-0

helm repo add kafbat-ui https://kafbat.github.io/helm-charts

cat > "$HELM_BASE_DIR/strimzi-kafka/kafka-ui.yaml" << EOF
yamlApplicationConfig:
  kafka:
    clusters:
      - name: kafka-cluster
        bootstrapServers: kafka-cluster-kafka-bootstrap.py-spark.svc.cluster.local:9092
  auth:
    type: disabled
  management:
    health:
      ldap:
        enabled: false
EOF

helm upgrade --install kafbat-ui kafbat-ui/kafka-ui -f "$HELM_BASE_DIR/strimzi-kafka/kafka-ui.yaml" -n py-spark --wait

export KAFKA_UI_POD=$(kubectl get pods --namespace py-spark -l "app.kubernetes.io/name=kafka-ui,app.kubernetes.io/instance=kafbat-ui" -o jsonpath="{.items[0].metadata.name}" |  head -n 1)
kubectl -n "$NAMESPACE" port-forward "$KAFKA_UI_POD" 8084:8080  >/dev/null 2>&1 &
PID=$!
echo "$KAFKA_UI_POD port-forward PID is $PID"