# As tres funcoes usam o mesmo modulo: role propria, Deny de escrita em bronze,
# log group com retencao e a funcao. O que muda entre elas vive aqui.
locals {
  lambdas = {
    fx = {
      description = "Ingere cotacoes do BCE via Frankfurter e materializa a tabela densa de cambio"
      timeout     = 300
      memory      = 512
      environment = {
        LAKEHOUSE_BUCKET    = var.bucket_name
        FX_QUOTE_CURRENCIES = "USD,EUR"
        FX_START_DATE       = "2023-07-25"
        DQ_REFERENCE_DATE   = var.reference_date
      }
    }
    silver = {
      description = "bronze -> silver: valida, deduplica, quarentena, tipa e converte para BRL"
      timeout     = 300
      memory      = 1024
      environment = {
        LAKEHOUSE_BUCKET  = var.bucket_name
        DQ_REFERENCE_DATE = var.reference_date
        DQ_FAIL_ON_ERROR  = "true"
      }
    }
    gold = {
      description = "silver -> gold: star schema com 3 dimensoes e 3 fatos"
      timeout     = 300
      memory      = 1024
      environment = {
        LAKEHOUSE_BUCKET = var.bucket_name
        REFERENCE_DATE   = var.reference_date
        DQ_FAIL_ON_ERROR = "true"
      }
    }
  }
}

module "lambda" {
  source   = "./modules/lambda"
  for_each = local.lambdas

  name               = "${var.project}-${each.key}"
  description        = each.value.description
  runtime            = var.lambda_runtime
  timeout            = each.value.timeout
  memory_size        = each.value.memory
  layers             = [var.pandas_layer_arn]
  package_path       = "${var.artifacts_dir}/flutter-${each.key}.zip"
  environment        = each.value.environment
  bucket_arn         = aws_s3_bucket.lakehouse.arn
  log_retention_days = var.log_retention_days
}
