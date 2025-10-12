#!/bin/bash

NAMESPACE=$1
SERVICE_NAME=$2
REPOSITORY=$3
TAG=$4
PORT=$5
ENVS=$6

helm create "$SERVICE_NAME"

source "$HELM_BASE_DIR/scripts/define_service.sh" "$SERVICE_NAME"

rm "$HELM_BASE_DIR/$SERVICE_NAME/templates/deployment.yaml"

cat > "$HELM_BASE_DIR/$SERVICE_NAME/templates/deployment.yaml" << EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "$SERVICE_NAME.fullname" . }}
  labels:
    {{- include "$SERVICE_NAME.labels" . | nindent 4 }}
spec:
  {{- if not .Values.autoscaling.enabled }}
  replicas: {{ .Values.replicaCount }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "$SERVICE_NAME.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      {{- with .Values.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      labels:
        {{- include "$SERVICE_NAME.labels" . | nindent 8 }}
        {{- with .Values.podLabels }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
    spec:
      {{- with .Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      serviceAccountName: {{ include "$SERVICE_NAME.serviceAccountName" . }}
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
            - name: $SERVICE_NAME{{ .port }}
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

read -r -d '' OVERRIDE_VALUES << EOF
replicaCount: 1
nameOverride: $SERVICE_NAME
fullnameOverride: $SERVICE_NAME

image:
  repository: $REPOSITORY
  tag: $TAG
  pullPolicy: IfNotPresent

containers:
  ports:
    - port: $PORT
      protocol: TCP

service:
  type: ClusterIP
  ports:
    - name: $SERVICE_NAME
      port: $PORT
      targetPort: $PORT

$ENVS
EOF

python update_values.py -r "$SERVICE_NAME" -p "$HELM_BASE_DIR/$SERVICE_NAME/values.yaml" -v "$OVERRIDE_VALUES"

set -e
POD_NAME=$(kubectl get pods --namespace py-spark -l "app.kubernetes.io/name=fake,app.kubernetes.io/instance=fake-release" -o jsonpath="{.items[0].metadata.name}"|  head -n 1)
kubectl -n "$NAMESPACE" delete pod "$POD_NAME"
set +e
helm upgrade --install "$SERVICE_NAME-release" "./$SERVICE_NAME" --namespace "$NAMESPACE"