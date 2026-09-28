{{- define "olc.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: orbit-link-controller
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "olc.selector" -}}
app.kubernetes.io/name: orbit-link-controller
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "olc.name" -}}{{ .Release.Name }}-controller{{- end -}}

{{- define "olc.secret" -}}
{{- if .Values.orbit.existingSecret -}}{{ .Values.orbit.existingSecret }}{{- else -}}{{ .Release.Name }}-orbit-api{{- end -}}
{{- end -}}

{{- define "olc.args" -}}
- --cluster-name={{ required "clusterName is required" .Values.clusterName }}
- --release={{ .Release.Name }}
- --orbit-api={{ .Values.orbit.api }}
- --orbit-token-file=/var/run/orbit/api-token
- --api-qps={{ .Values.orbit.apiQPS }}
- --api-burst={{ .Values.orbit.apiBurst }}
- --ingress-class={{ .Values.ingressClass.name }}
- --controller-name={{ .Values.controllerName }}
- --connector-image={{ .Values.connector.image }}
- --connector-replicas={{ .Values.connector.replicas }}
{{- if .Values.connector.tcp }}
- --connector-tcp
{{- end }}
{{- with .Values.orbit.landing }}
- --landing={{ . }}
{{- end }}
{{- with .Values.orbit.landing2 }}
- --landing2={{ . }}
{{- end }}
{{- if .Values.orbit.adopt }}
- --adopt
{{- end }}
- --zap-log-level={{ .Values.logLevel }}
{{- end -}}

{{- define "olc.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}{{- end -}}
