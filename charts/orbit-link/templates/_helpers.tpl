{{- define "orbit-link.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "orbit-link.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "orbit-link.selectorLabels" -}}
app.kubernetes.io/name: {{ include "orbit-link.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "orbit-link.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{ include "orbit-link.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "orbit-link.secretName" -}}
{{- if .Values.tunnel.existingSecret -}}
{{- .Values.tunnel.existingSecret -}}
{{- else -}}
{{- include "orbit-link.fullname" . -}}
{{- end -}}
{{- end -}}

{{/* drain + 5 s, in whole seconds */}}
{{- define "orbit-link.grace" -}}
{{- $d := .Values.drain | toString -}}
{{- $s := 8 -}}
{{- if hasSuffix "ms" $d -}}{{- $s = div (trimSuffix "ms" $d | int) 1000 -}}
{{- else if hasSuffix "s" $d -}}{{- $s = trimSuffix "s" $d | int -}}
{{- else if hasSuffix "m" $d -}}{{- $s = mul (trimSuffix "m" $d | int) 60 -}}
{{- end -}}
{{- add $s 5 -}}
{{- end -}}
