#!/bin/bash

SERVICE_NAME=$1

rm "$HELM_BASE_DIR/$SERVICE_NAME/templates/deployment.yaml"

cat > "$HELM_BASE_DIR/$SERVICE_NAME/templates/statefulset.yaml" << EOF
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ include "$SERVICE_NAME.fullname" . }}
  labels:
    {{- include "$SERVICE_NAME.labels" . | nindent 4 }}
spec:
  serviceName: {{ include "$SERVICE_NAME.fullname" . }}-headless
  replicas: {{ .Values.replicaCount }}
  selector:
    matchLabels:
      {{- include "$SERVICE_NAME.selectorLabels" . | nindent 8 }}
  template:
    metadata:
      labels:
        {{- include "$SERVICE_NAME.selectorLabels" . | nindent 12 }}
    spec:
      containers:
        - name: {{ .Chart.Name }}
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          {{- if hasKey .Values.image "command" }}
          command:
          {{- range .Values.image.command }}
            - {{ . | quote }}
          {{- end }}
          {{- end }}
          {{- if hasKey .Values.image "args" }}
          args:
          {{- range .Values.image.args }}
            - {{ . | quote }}
          {{- end }}
          {{- end }}
          {{- if hasKey .Values.containers "ports" }}
          ports:
          {{- range \$port := .Values.containers.ports }}
            - containerPort: {{ \$port }}
          {{- end }}
          {{- end }}
          volumeMounts:
            - name: $SERVICE_NAME-volume
              mountPath: {{ .Values.persistence.mountPath }}
          {{- if hasKey .Values "env" }}
          env:
          {{- range \$key, \$value := .Values.env }}
          - name: {{ \$key | quote }}
            value: {{ \$value | quote }}
          {{- end }}
          {{- end }}
  volumeClaimTemplates:
    - metadata:
        name: $SERVICE_NAME-volume
      spec:
        accessModes: [ "ReadWriteOnce" ]
        resources:
          requests:
            storage: {{ .Values.persistence.size }}
EOF