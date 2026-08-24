output "bucket" {
  description = "Bucket do lakehouse"
  value       = aws_s3_bucket.lakehouse.bucket
}

output "lambda_functions" {
  description = "Nome das lambdas criadas"
  value       = { for k, m in module.lambda : k => m.function_name }
}

output "state_machine_arn" {
  description = "ARN da state machine"
  value       = aws_sfn_state_machine.pipeline.arn
}

output "athena_workgroup" {
  description = "Workgroup do Athena, ja com output location configurado"
  value       = aws_athena_workgroup.martech.name
}

output "comando_disparar_pipeline" {
  description = "Executa o pipeline agora, sem esperar o schedule"
  value       = "aws stepfunctions start-execution --state-machine-arn ${aws_sfn_state_machine.pipeline.arn} --input '{\"reference_date\":\"${var.reference_date}\"}' --region ${var.region}"
}
