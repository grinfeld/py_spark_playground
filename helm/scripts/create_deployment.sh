#!/bin/bash

TAG=""
PORTFORWARD=""
PORT=""
ENVS=""
IMAGE=""
SERVICE=""
ENVS=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    -n=*)
      NAMESPACE="${1#*=}"
      ;;
    -t=*)
      TAG="${1#*=}"
      ;;
    -i=*)
      IMAGE="${1#*=}"
      ;;
    -s=*)
      SERVICE="${1#*=}"
      ;;
    -p=*)
      PORT="${1#*=}"
      ;;
    -pf=*)
      PORTFORWARD="${1#*=}"
      ;;
    *)
      ENVS="$@"
      break
      ;;
  esac
  shift
done

if [[ "$SERVICE" == "" ]]; then
  echo "You can't define deployment without setting service name"
fi

echo -e "-------------------------"
echo -e "--- CREATE $SERVICE ---"
echo -e "-------------------------"

if [[ "$TAG" == "" ]]; then
  echo "You should specify tag"
fi

if [[ "$IMAGE" == "" ]]; then
  echo "You should specify image"
fi

echo -e "Starting $SERVICE deployment creation with image $IMAGE:$TAG with port=$PORT and env vars $ENV. \nAfter finish create port-forwarding = $PORTFORWARD"

helm create "$SERVICE"

source "$HELM_BASE_DIR/scripts/define_service.sh" "$SERVICE"

rm "$HELM_BASE_DIR/$SERVICE/templates/deployment.yaml"

cat > "$HELM_BASE_DIR/$SERVICE/templates/deployment.yaml" << EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "$SERVICE.fullname" . }}
  labels:
    {{- include "$SERVICE.labels" . | nindent 4 }}
spec:
  {{- if not .Values.autoscaling.enabled }}
  replicas: {{ .Values.replicaCount }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "$SERVICE.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      {{- with .Values.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      labels:
        {{- include "$SERVICE.labels" . | nindent 8 }}
        {{- with .Values.podLabels }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
    spec:
      {{- with .Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      serviceAccountName: {{ include "$SERVICE.serviceAccountName" . }}
      {{- with .Values.podSecurityContext }}
      securityContext:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      containers:
        - name: {{ .Chart.Name }}
          {{- with .Values.securityContext }}
          securityContext:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          {{- if hasKey .Values.containers "ports" }}
          ports:
          {{- range .Values.containers.ports }}
            - name: $SERVICE{{ .port }}
              containerPort: {{ .port }}
              protocol: {{ .protocol | quote  }}
          {{- end }}
          {{- end }}
          {{- with .Values.livenessProbe }}
          livenessProbe:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- with .Values.readinessProbe }}
          readinessProbe:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- with .Values.resources }}
          resources:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- with .Values.volumeMounts }}
          volumeMounts:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- if hasKey .Values "env" }}
          env:
          {{- range \$key, \$value := .Values.env }}
          - name: {{ \$key | quote }}
            value: {{ \$value | quote }}
          {{- end }}
          {{- end }}
      {{- with .Values.volumes }}
      volumes:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
EOF

OVERRIDE_VALUES=$(cat <<EOF
replicaCount: 1
nameOverride: $SERVICE
fullnameOverride: $SERVICE

image:
  repository: $IMAGE
  tag: $TAG
  pullPolicy: IfNotPresent

containers:
  ports:
    - port: $PORT
      protocol: TCP

service:
  type: ClusterIP
  ports:
    - name: $SERVICE
      port: $PORT
      targetPort: $PORT

$ENVS
EOF
)

python update_values.py -r "$SERVICE" -p "$HELM_BASE_DIR/$SERVICE/values.yaml" -v "$OVERRIDE_VALUES"

set +e
POD_NAME=$(kubectl get pods --namespace py-spark -l "app.kubernetes.io/name=$SERVICE,app.kubernetes.io/instance=$SERVICE-release" -o jsonpath="{.items[0].metadata.name}"|  head -n 1)
kubectl -n "$NAMESPACE" delete pod "$POD_NAME"
set -e

helm upgrade --install "$SERVICE-release" "./$SERVICE" --namespace "$NAMESPACE" --values "$HELM_BASE_DIR/$SERVICE/values.yaml"

echo "Finished $SERVICE. Now starting port-forwarding"

if [[ "$PORT" != "" ]]; then
  if [[ "$PORTFORWARD" != "" ]]; then
    source "$HELM_BASE_DIR/scripts/make_port_forward.sh" "$NAMESPACE" "$SERVICE" "$PORT" "$PORTFORWARD" "app.kubernetes.io/name=$SERVICE,app.kubernetes.io/instance=$SERVICE-release"
  fi
fi
