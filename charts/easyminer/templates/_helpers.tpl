{{- define "easyminer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "easyminer.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "easyminer.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "easyminer.labels" -}}
helm.sh/chart: {{ include "easyminer.chart" . }}
{{ include "easyminer.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "easyminer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "easyminer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "easyminer.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "easyminer.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "easyminer.redisHost" -}}
{{- printf "%s-redis" (include "easyminer.fullname" .) }}
{{- end }}

{{- define "easyminer.celeryBrokerUrl" -}}
{{- if .Values.redis.enabled }}
{{- printf "redis://%s:6379/0" (include "easyminer.redisHost" .) }}
{{- else }}
{{- required "externalRedis.brokerUrl is required when redis.enabled=false" .Values.externalRedis.brokerUrl }}
{{- end }}
{{- end }}

{{- define "easyminer.celeryBackendUrl" -}}
{{- if .Values.redis.enabled }}
{{- printf "redis://%s:6379/1" (include "easyminer.redisHost" .) }}
{{- else }}
{{- required "externalRedis.backendUrl is required when redis.enabled=false" .Values.externalRedis.backendUrl }}
{{- end }}
{{- end }}
