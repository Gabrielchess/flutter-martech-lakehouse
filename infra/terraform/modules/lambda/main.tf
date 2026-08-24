data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# Uma role por funcao. Role compartilhada economiza linhas de Terraform e
# custa a capacidade de responder "quem escreveu esse objeto?" no CloudTrail.
resource "aws_iam_role" "this" {
  name               = "${var.name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "lakehouse" {
  statement {
    sid       = "LerLakehouse"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [var.bucket_arn, "${var.bucket_arn}/*"]
  }

  statement {
    sid       = "EscreverCamadasDerivadas"
    actions   = ["s3:PutObject"]
    resources = ["${var.bucket_arn}/*"]
  }

  # Bronze e imutavel para quem TRANSFORMA. Sem este Deny, um bug de prefixo
  # na silver sobrescreve a origem e o reprocesso do zero deixa de existir.
  statement {
    sid       = "BronzeImutavel"
    effect    = "Deny"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${var.bucket_arn}/bronze/*"]
  }
}

resource "aws_iam_role_policy" "lakehouse" {
  name   = "lakehouse"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.lakehouse.json
}

resource "aws_iam_role_policy_attachment" "logs" {
  role       = aws_iam_role.this.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Criado explicitamente para a retencao valer. Se a Lambda criar o grupo
# sozinha na primeira invocacao, ele nasce com retencao infinita.
resource "aws_cloudwatch_log_group" "this" {
  name              = "/aws/lambda/${var.name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "this" {
  function_name = var.name
  description   = var.description
  role          = aws_iam_role.this.arn
  handler       = var.handler
  runtime       = var.runtime
  timeout       = var.timeout
  memory_size   = var.memory_size
  layers        = var.layers

  filename         = var.package_path
  source_code_hash = filebase64sha256(var.package_path)

  environment {
    variables = var.environment
  }

  depends_on = [
    aws_cloudwatch_log_group.this,
    aws_iam_role_policy_attachment.logs,
  ]
}
