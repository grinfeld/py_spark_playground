#!/bin/bash

NAMESPACE=$1

helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

mkdir -p "$HELM_BASE_DIR/spark/"

# Note:
# webhook.securityContext.runAsNonRoot: false - this is mandatory.
# When webhooks starts not as root, it fails - no clue why..
# Tried different staff - nothing worked, so stayed with it.
# controller.securityContext.runAsNonRoot: true
# controller.securityContext.runAsUser: 185
# user UID in spark docker container is 185
# both 2 settings above - fix starting the controller pod. Opposite to webhook - it requires nonRoot user.
# spark.jobNamespaces: [$NAMESPACE] - defines what namespace spark operator should launch spark pods (driver and executors)
# spark.namespaces: [$NAMESPACE] - defines what namespace spark operator should watch for event. It could be more than one namespace
cat > "$HELM_BASE_DIR/spark/values.yaml" << EOF
controller:
  securityContext:
    runAsNonRoot: true
    runAsUser: 185
webhook:
  securityContext:
    runAsNonRoot: false
spark:
  jobNamespaces:
    - $NAMESPACE
  namespaces:
    - $NAMESPACE
  serviceAccount:
    create: true
    name: spark
sparkJob:
  rbac:
    create: true
EOF

helm upgrade --install spark spark-operator/spark-operator --namespace "$NAMESPACE" --values "$HELM_BASE_DIR/spark/values.yaml" --wait

# We need to create roles. Naming is important:
# subjects:
#   - kind: ServiceAccount
#     name: spark-spark-operator-webhook
#     namespace: $NAMESPACE

# subjects:
#   - kind: ServiceAccount
#     name: spark-spark-operator-controller
#     namespace: $NAMESPACE

# spark-operator creates roles by using name from
# 'helm upgrade --install spark spark-operator/spark-operator ....' - here the name is spark
# and appends '-spark-operator-controller' and 'spark-spark-operator-webhook'

cat <<EOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-operator-event-creator
  namespace: $NAMESPACE
rules:
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-operator-controller-event-creator-binding
  namespace: $NAMESPACE
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: spark-operator-event-creator
subjects:
  - kind: ServiceAccount
    name: spark-spark-operator-controller
    namespace: $NAMESPACE
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-operator-webhook-event-creator-binding
  namespace: $NAMESPACE
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: spark-operator-event-creator
subjects:
  - kind: ServiceAccount
    name: spark-spark-operator-webhook
    namespace: $NAMESPACE
EOF

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: spark-config
  namespace: $NAMESPACE
data:
  STAM: stam
EOF