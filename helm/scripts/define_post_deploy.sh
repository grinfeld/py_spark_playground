#!/bin/bash

SERVICE_NAME=$1

cat > "$HELM_BASE_DIR/$SERVICE_NAME/templates/post-deploy-job.yaml" << EOF
# ConfigMap for the post-deployment script
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "$SERVICE_NAME.fullname" . }}-post-deploy-script
  labels:
    {{- include "$SERVICE_NAME.labels" . | nindent 4 }}
  # Annotation ensures the ConfigMap is available when the Job is created.
  annotations:
    "helm.sh/hook": post-install,post-upgrade
    "helm.sh/hook-weight": "0"  # Set weight to 0
data:
  post-deploy.sh: |
{{ .Values.post.deploy.script | indent 4 }}
---
# Job to execute the post-deployment script
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "$SERVICE_NAME.fullname" . }}-post-deploy-job
  labels:
    {{- include "$SERVICE_NAME.labels" . | nindent 4 }}
  annotations:
    # Use the post-install and post-upgrade hooks to run after deployment.
    "helm.sh/hook": post-install,post-upgrade
    # This policy deletes the Job and its Pod after it completes successfully.
    "helm.sh/hook-delete-policy": {{ .Values.post.deploy.hook.delete.policy | quote }}
    "helm.sh/hook-weight": "1"  # Set weight to 1
spec:
  ttlSecondsAfterFinished: 100
  template:
    metadata:
      labels:
        {{- include "$SERVICE_NAME.selectorLabels" . | nindent 8 }}
    spec:
      restartPolicy: OnFailure
      containers:
        - name: post-deploy-container
          image: "{{ .Values.post.deploy.image.repository }}:{{ .Values.post.deploy.image.tag }}"
          command: ["/bin/sh", "/scripts/post-deploy.sh"]
          env:
          {{- if hasKey .Values "env" }}
          {{- range \$key, \$value := .Values.env }}
          - name: {{ \$key | quote }}
            value: {{ \$value | quote }}
          {{- end }}
          {{- end }}
          {{- if hasKey .Values.post.deploy "env" }}
          {{- range \$key, \$value := .Values.post.deploy.env }}
          - name: {{ \$key | quote }}
            value: {{ \$value | quote }}
          {{- end }}
          {{- end }}
          volumeMounts:
            - name: post-deploy-script
              mountPath: /scripts
          {{- if hasKey .Values.post.deploy.hostPathVolume "hostVolumes" }}
          {{- range .Values.post.deploy.hostPathVolume.hostVolumes }}
            - name: {{ .name | quote }}
              mountPath: {{ .mountPath }}
          {{- end }}
          {{- end }}
      volumes:
        - name: post-deploy-script
          configMap:
            name: {{ include "$SERVICE_NAME.fullname" . }}-post-deploy-script
            defaultMode: 0755
        {{- if hasKey .Values.post.deploy.hostPathVolume "hostVolumes" }}
        {{- range .Values.post.deploy.hostPathVolume.hostVolumes }}
        - name: {{ .name | quote }}
          hostPath:
            path: {{ .path | quote }}
            type: DirectoryOrCreate
        {{- end }}
        {{- end }}
EOF