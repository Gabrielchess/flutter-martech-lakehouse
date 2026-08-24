variable "name" {
  type = string
}

variable "description" {
  type = string
}

variable "handler" {
  type    = string
  default = "handler.lambda_handler"
}

variable "runtime" {
  type = string
}

variable "timeout" {
  type    = number
  default = 300
}

variable "memory_size" {
  type    = number
  default = 512
}

variable "layers" {
  type    = list(string)
  default = []
}

variable "package_path" {
  description = "Caminho do zip da funcao"
  type        = string
}

variable "environment" {
  type    = map(string)
  default = {}
}

variable "bucket_arn" {
  description = "ARN do bucket do lakehouse"
  type        = string
}

variable "log_retention_days" {
  type    = number
  default = 30
}
