# =============================================================================
# Lakehouse — o bucket unico, com bronze / silver / gold / quality / reference
# =============================================================================
resource "aws_s3_bucket" "lakehouse" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Rede de seguranca do overwrite: silver e gold sao gravadas com PutObject
# idempotente, entao uma execucao ruim sobrescreve a boa.
resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket     = aws_s3_bucket.lakehouse.id
  depends_on = [aws_s3_bucket_versioning.lakehouse]

  rule {
    id     = "expirar-resultados-athena"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = 7
    }
  }

  rule {
    id     = "limitar-versoes-antigas"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# =============================================================================
# Catalogo — Glue Data Catalog + workgroup do Athena
# =============================================================================
resource "aws_glue_catalog_database" "martech" {
  name        = var.glue_database
  description = "Catalogo das camadas silver e gold do lakehouse Martech"
}

resource "aws_athena_workgroup" "martech" {
  name = var.project

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    # O dataset tem 250 jogadores; query que passe de 1 GB e cross join
    # acidental, nao analise.
    bytes_scanned_cutoff_per_query = 1073741824

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  force_destroy = true
}
