variable "region" {
  description = "Regiao AWS"
  type        = string
  default     = "us-east-2"
}

variable "project" {
  description = "Prefixo de todos os recursos"
  type        = string
  default     = "flutter-martech"
}

variable "environment" {
  description = "dev | prod"
  type        = string
  default     = "dev"
}

variable "bucket_name" {
  description = "Nome do bucket do lakehouse. Precisa ser globalmente unico."
  type        = string
  default     = "flutter-martech-lakehouse"
}

variable "glue_database" {
  description = "Database do Glue Data Catalog usado pelo Athena"
  type        = string
  default     = "flutter_martech"
}

variable "lambda_runtime" {
  type    = string
  default = "python3.13"
}

variable "pandas_layer_arn" {
  description = <<-EOT
    Layer gerenciada pela AWS com pandas + pyarrow. A conta 336392948345 e a
    oficial da AWS na maioria das regioes. Confira a versao:
    aws lambda list-layer-versions --layer-name AWSSDKPandas-Python313
  EOT
  type        = string
  default     = "arn:aws:lambda:us-east-2:336392948345:layer:AWSSDKPandas-Python313:1"
}

variable "artifacts_dir" {
  description = "Pasta com os zips das lambdas, relativa a raiz do terraform"
  type        = string
  default     = "../dist"
}

variable "reference_date" {
  description = "Data de referencia do case. Toda regua temporal sai daqui."
  type        = string
  default     = "2024-04-01"
}

variable "schedule_expression" {
  description = "Quando o pipeline roda. Mensal, dia 1 as 03:00 UTC."
  type        = string
  default     = "cron(0 3 1 * ? *)"
}

variable "schedule_enabled" {
  description = "false deixa o schedule criado porem desligado"
  type        = bool
  default     = false
}

variable "alarm_actions" {
  description = <<-EOT
    ARNs notificados quando um alarme dispara (topico SNS, Chatbot, etc).
    Vazio = os alarmes existem e ficam vermelhos no console, mas nao avisam
    ninguem. Preencha quando houver um canal de alerta.
  EOT
  type        = list(string)
  default     = []
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "dq_namespace" {
  description = "Namespace EMF emitido pelas lambdas (shared/logger.py)"
  type        = string
  default     = "FlutterMartech/DataQuality"
}
