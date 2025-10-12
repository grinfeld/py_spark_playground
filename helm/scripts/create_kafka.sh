#!/bin/bash

NAMESPACE=$1
TAG=$2

echo -e "-------------------------"
echo -e "----------KAFKA----------"
echo -e "-------------------------"

mkdir -p "$HELM_BASE_DIR/strimzi-kafka/"

cat > "$HELM_BASE_DIR/strimzi-kafka/operator-values.yaml" << EOF
  watchNamespaces:
    - $NAMESPACE
EOF

set +e
helm upgrade --install strimzi-kafka-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --namespace "$NAMESPACE" \
  --values "$HELM_BASE_DIR/strimzi-kafka/operator-values.yaml" \
  --wait
set -e
# Install operator and wait until it's ready
kubectl wait --for=condition=ready pod -l name=strimzi-cluster-operator -n "$NAMESPACE" --timeout=300s

echo -e "-------------------------"
echo -e "------KAFKA CLUSTER------"
echo -e "-------------------------"

# Install Kafka Cluster
echo "Install Kafka Cluster"

helm create kafka-cluster-crd

rm -rf "$HELM_BASE_DIR/kafka-cluster-crd/templates/"

mkdir -p "$HELM_BASE_DIR/kafka-cluster-crd/templates/"

cat > "$HELM_BASE_DIR/kafka-cluster-crd/templates/nodepools.yaml" << EOF
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaNodePool
metadata:
  name: single-node
  labels:
    strimzi.io/cluster: {{ .Values.kafkaCluster.name }}
spec:
  replicas: {{ .Values.kafkaCluster.nodePool.replicas }}
  roles:
    {{- toYaml .Values.kafkaCluster.nodePool.roles | nindent 4 }}
  storage:
    type: jbod
    volumes:
      - id: 0
        type: {{ .Values.kafkaCluster.nodePool.storage.type }} # persistent-claim or ephemeral
        {{- if eq .Values.kafkaCluster.nodePool.storage.type "persistent-claim" }}
        size: {{ .Values.kafkaCluster.nodePool.storage.size }}
        deleteClaim: {{ .Values.kafkaCluster.nodePool.storage.deleteClaim }}
        kraftMetadata: shared
        {{- end }}
EOF

cat > "$HELM_BASE_DIR/kafka-cluster-crd/templates/kafka-resources.yaml" << EOF
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: {{ .Values.kafkaCluster.name }}
spec:
  kafka:
    version: {{ .Values.kafkaCluster.version }}
    metadataVersion: {{ .Values.kafkaCluster.metadataVersion }}
    listeners:
      - name: plain
        port: {{ .Values.kafkaCluster.listeners.plain.port }}
        type: {{ .Values.kafkaCluster.listeners.plain.type }}
        tls: false
      - name: tls
        port: {{ .Values.kafkaCluster.listeners.tls.port }}
        type: {{ .Values.kafkaCluster.listeners.tls.type }}
        tls: true
      - name: external
        port: {{ .Values.kafkaCluster.listeners.external.port }}
        type: {{ .Values.kafkaCluster.listeners.external.type }}
        tls: false
        configuration:
          bootstrap:
            nodePort: {{ .Values.kafkaCluster.listeners.external.nodePort }}
    config:
{{ toYaml .Values.kafkaCluster.config | nindent 6 }}
  entityOperator:
    topicOperator: {}
    userOperator: {}
---
# Loop through the list of topics defined in values.yaml
{{- range .Values.kafkaTopics }}
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: {{ .name }}
  labels:
    strimzi.io/cluster: {{ $.Values.kafkaCluster.name }}
spec:
  partitions: {{ .partitions }}
  replicas: {{ .replicas }}
  config:
{{ toYaml .config | nindent 4 }}
---
{{- end }}
EOF

cat > "$HELM_BASE_DIR/kafka-cluster-crd/values.yaml" << EOF
kafkaCluster:
  clusterId: kafka
  name: kafka
  version: 4.0.0
  metadataVersion: 4.1-IV1
  listeners:
    plain:
      port: 9092
      type: internal
    tls:
      port: 9093
      type: internal
    external:
      port: 9094
      type: nodeport
      nodePort: 32100
  config:
    offsets.topic.replication.factor: 1
    transaction.state.log.replication.factor: 1
    transaction.state.log.min.insync.replicas: 1
    default.replication.factor: 1
    min.insync.replicas: 1
  nodePool:
    replicas: 1
    roles: [controller, broker]
    storage:
      type: persistent-claim
      size: 10Gi
      deleteClaim: true

# Configuration for the Kafka topics
kafkaTopics:
  - name: source-topic
    partitions: 1
    replicas: 1
    config:
      retention.ms: 604800000
EOF

# Apply the Kafka cluster (not before operator is ready)
helm upgrade --install kafka-cluster-crd ./kafka-cluster-crd -n "$NAMESPACE"

# Wait for Kafka cluster to be ready
kubectl wait kafka/kafka --for=condition=Ready --timeout=300s -n "$NAMESPACE"

KAFKA_BOOTSTRAP=$(kubectl -n py-spark get kafka kafka \
  -o jsonpath='{range .status.listeners[*]}{.type}{"\t"}{.addresses[0].host}{":"}{.addresses[0].port}{"\n"}{end}' \
  | grep 9092 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')

echo -e "-------------------------"
echo -e "------KAFKA CONNECT------"
echo -e "-------------------------"

# Install Kafka-Connect
helm create kafka-connect-crd

rm -rf "$HELM_BASE_DIR/kafka-connect-crd/templates/"

mkdir -p "$HELM_BASE_DIR/kafka-connect-crd/templates/"

cat > "$HELM_BASE_DIR/kafka-connect-crd/templates/kafka-connect-resources.yaml" << EOF
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnect
metadata:
  name: {{ .Values.kafkaConnect.name }}
  {{- if hasKey .Values.kafkaConnect "createConnector" }}
  annotations:
    strimzi.io/use-connector-resources: "{{ .Values.kafkaConnect.createConnector }}"
  {{- end }}
spec:
  version: {{ .Values.kafkaConnect.version }}
  replicas: {{ .Values.kafkaConnect.replicas }}
  bootstrapServers: {{ .Values.kafkaConnect.bootstrap | quote }}
  {{- if hasKey .Values.kafkaConnect "tmpDirSizeLimit" }}
  template:
    pod:
      tmpDirSizeLimit: {{ .Values.kafkaConnect.tmpDirSizeLimit | quote }}
  {{- end }}
  livenessProbe:
    initialDelaySeconds: 120
    periodSeconds: 60
    timeoutSeconds: 5
    failureThreshold: 24
  readinessProbe:
    initialDelaySeconds: 120
    periodSeconds: 60
    timeoutSeconds: 5
    failureThreshold: 24
  {{- if hasKey .Values.kafkaConnect "image" }}
  image: {{ .Values.kafkaConnect.image | default "" | quote }}
  {{- if hasKey .Values.kafkaConnect "resources" }}
  resources:
{{ toYaml .Values.kafkaConnect.resources | nindent 4 }}
  {{- end }}
  {{- if hasKey .Values.kafkaConnect "jvmOptions" }}
  jvmOptions:
{{ toYaml .Values.kafkaConnect.jvmOptions | nindent 4 }}
  {{- end }}
  {{- end }}
  config:
{{ toYaml .Values.kafkaConnect.config | nindent 4 }}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  name: {{ .Values.kafkaConnect.name }}
  labels:
    strimzi.io/cluster: {{ .Values.kafkaConnect.name }}
spec:
  class: {{ .Values.kafkaConnector.class | quote }}
  tasksMax: {{ .Values.kafkaConnector.tasksMax }}
  config:
{{ toYaml .Values.kafkaConnector.config | nindent 4 }}
EOF

cat > "$HELM_BASE_DIR/kafka-connect-crd/values.yaml" << EOF
kafkaConnect:
  name: iceberg-kafka-connect
  version: 4.0.0
  replicas: 1
  image: py-spark-kconnect:$TAG
  bootstrap: $KAFKA_BOOTSTRAP
  metadataVersion: 4.0-IV1
  createConnector: true
  tmpDirSizeLimit: 5Gi
  resources:
    requests:
      cpu: "500m"
      memory: "2Gi"
    limits:
      cpu: "1"
      memory: "3Gi"
  jvmOptions:
    -Xms: 512m
    -Xmx: 2g
  config:
    group.id: iceberg-kafka-connect
    offset.storage.topic: iceberg-kafka-cluster-offsets
    config.storage.topic: iceberg-kafka-cluster-configs
    status.storage.topic: iceberg-kafka-cluster-status
    # -1 means it will use the default replication factor configured in the broker
    config.storage.replication.factor: 1
    offset.storage.replication.factor: 1
    status.storage.replication.factor: 1

kafkaConnector:
  class: org.apache.iceberg.connect.IcebergSinkConnector
  tasksMax: 2
  config:
    topics: source-topic
    iceberg.catalog: mycatalog
    iceberg.catalog.type: hadoop
    iceberg.tables.auto-create-enabled: true
    iceberg.tables.evolve-schema-enabled: true
    iceberg.tables: demo.customers
    iceberg.table.demo.customers.partition-by: month(Subscription_Date),Country
    iceberg.catalog.warehouse: s3a://mycatalog
    iceberg.catalog.s3.access-key-id: minioadmin
    iceberg.catalog.s3.secret-access-key: minioadmin
    iceberg.catalog.s3.endpoint: http://minio:9000
    iceberg.catalog.s3.path-style-access: true
    iceberg.catalog.s3.ssl-enabled: false
    iceberg.catalog.s3.signing-region: us-east-1
    iceberg.hadoop.fs.s3a.endpoint: http://minio:9000
    iceberg.hadoop.fs.s3a.access.key: minioadmin
    iceberg.hadoop.fs.s3a.secret.key: minioadmin
    iceberg.hadoop.fs.s3a.path.style.access: true
    iceberg.hadoop.fs.s3a.connection.ssl.enabled: false
    iceberg.hadoop.fs.s3a.fast.upload.buffer: bytebuffer
    iceberg.hadoop.fs.file.impl: org.apache.hadoop.fs.LocalFileSystem
    iceberg.hadoop.fs.AbstractFileSystem.file.impl: org.apache.hadoop.fs.local.LocalFs
    iceberg.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
    iceberg.hadoop.fs.s3a.buffer.dir: /tmp
    value.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter.schemas.enable: false
    key.converter: org.apache.kafka.connect.converters.ByteArrayConverter
    key.converter.schemas.enable: false
    transforms.TimestampConverter.type: org.apache.kafka.connect.transforms.TimestampConverter$Value
    transforms.TimestampConverter.field: Subscription_Date
    transforms.TimestampConverter.unix.precision: seconds
    transforms.TimestampConverter.target.type: Timestamp
    transforms: TimestampConverter
    iceberg.control.commit.interval-ms: 1000
EOF

helm upgrade --install kafka-connect-crd ./kafka-connect-crd -n "$NAMESPACE" --wait

sleep 5

# Wait for KafkaConnect to be ready (so that its pod exists before port-forwarding)
kubectl wait --for=condition=Ready pod -l strimzi.io/name=iceberg-kafka-connect-connect -n "$NAMESPACE" --timeout=300s

source "$HELM_BASE_DIR/scripts/make_port_forward.sh" "$NAMESPACE" "Kafka Connect" "8083" "8083" "strimzi.io/name=iceberg-kafka-connect-connect"

# Install Kafkabat-UI
helm repo add kafbat-ui https://kafbat.github.io/helm-charts

mkdir -p "$HELM_BASE_DIR/kafbat-ui/"

cat > "$HELM_BASE_DIR/kafbat-ui/values.yaml" << EOF
yamlApplicationConfig:
  kafka:
    clusters:
      - name: kafka-cluster
        bootstrapServers: $KAFKA_BOOTSTRAP
  auth:
    type: disabled
  management:
    health:
      ldap:
        enabled: false
EOF

helm upgrade --install kafbat-ui kafbat-ui/kafka-ui -f "$HELM_BASE_DIR/kafbat-ui/values.yaml" -n "$NAMESPACE" --wait

source "$HELM_BASE_DIR/scripts/make_port_forward.sh" "$NAMESPACE" "Kafka UI" "8080" "8084" "app.kubernetes.io/name=kafka-ui,app.kubernetes.io/instance=kafbat-ui"
