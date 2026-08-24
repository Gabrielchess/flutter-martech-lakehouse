data "aws_caller_identity" "current" {}

# =============================================================================
# Step Functions — fx -> silver -> gold, sequencial
# =============================================================================
data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${var.project}-statemachine-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "sfn" {
  statement {
    sid       = "InvocarLambdasDoPipeline"
    actions   = ["lambda:InvokeFunction"]
    resources = [for m in module.lambda : m.function_arn]
  }

  # Exigido pelo Step Functions para entregar log no CloudWatch. O servico nao
  # aceita restricao por recurso nessas acoes.
  statement {
    sid = "EntregarLogs"

    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "sfn" {
  name   = "pipeline"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn.json
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${var.project}-pipeline"
  retention_in_days = var.log_retention_days
}

# Sem Catch nos Task: nao ha canal de notificacao configurado, e um Catch que
# so leva a um Fail nao acrescenta nada — o erro original ja fica no historico
# da execucao, que ja termina em FAILED, que e o que o alarme observa.
# Quando existir um topico de alerta, entra Catch -> Publish -> Fail.
resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project}-pipeline"
  role_arn = aws_iam_role.sfn.arn

  definition = templatefile("${path.module}/statemachine.asl.tftpl", {
    fx_arn     = module.lambda["fx"].function_arn
    silver_arn = module.lambda["silver"].function_arn
    gold_arn   = module.lambda["gold"].function_arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }
}

# =============================================================================
# EventBridge Scheduler — dispara o pipeline
# =============================================================================
data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.project}-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

resource "aws_iam_role_policy" "scheduler" {
  name = "disparar-pipeline"
  role = aws_iam_role.scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.pipeline.arn
    }]
  })
}

resource "aws_scheduler_schedule" "pipeline" {
  name       = "${var.project}-mensal"
  state      = var.schedule_enabled ? "ENABLED" : "DISABLED"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.scheduler.arn

    # ingest_date fica de fora de proposito: a lambda usa a data de hoje quando
    # ele nao vem. Fixar aqui congelaria a particao de bronze numa data so.
    input = jsonencode({
      reference_date = var.reference_date
    })

    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}
