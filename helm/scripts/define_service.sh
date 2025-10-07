#!/bin/bash

SERVICE_NAME=$1

rm "$HELM_BASE_DIR/$SERVICE_NAME/templates/service.yaml"

cat > "$HELM_BASE_DIR/$SERVICE_NAME/templates/service.yaml" << EOF
apiVersion: v1
kind: Service
metadata:
  name: {{ include "$SERVICE_NAME.fullname" . }}
  labels:
    {{- include "$SERVICE_NAME.labels" . | nindent 4 }}
spec:
  type: {{ .Values.service.type }}
  {{- if hasKey .Values.service "ports" }}
  ports:
  {{- range .Values.service.ports }}
    - name: {{ .name | quote }}
      port: {{ .port }}
      targetPort: {{ .targetPort }}
  {{- end }}
  {{- end }}
  selector:
    {{- include "$SERVICE_NAME.selectorLabels" . | nindent 4 }}
EOF