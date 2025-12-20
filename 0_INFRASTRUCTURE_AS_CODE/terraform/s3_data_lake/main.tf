
# Production S3 Data Lake Configuration
# Layered architecture (Bronze/Silver/Gold)


terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "data-eng-terraform-state"
    key            = "production/data-lake/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-state-locks"
    encrypt        = true
  }
}


# Provider


provider "aws" {
  region = var.aws_region

  assume_role {
    role_arn = "arn:aws:iam::${var.account_id}:role/TerraformExecutionRole"
  }

  default_tags {
    tags = merge(
      var.tags,
      {
        Environment = var.environment
        Project     = "Data Platform"
        ManagedBy   = "Terraform"
        CostCenter  = "Data Engineering"
      }
    )
  }
}

############################################
# Logging bucket (centralized logs)
############################################

resource "aws_s3_bucket" "logs" {
  bucket = "${var.project_prefix}-logs-${var.environment}"

  tags = merge(
    var.tags,
    {
      Purpose = "logging"
    }
  )
}

resource "aws_s3_bucket_versioning" "logs" {
  count  = var.enable_versioning ? 1 : 0
  bucket = aws_s3_bucket.logs.id

  versioning_configuration {
    status = "Enabled"
  }
}

############################################
# KMS (optional encryption)
############################################

resource "aws_kms_key" "data_lake_key" {
  count                   = var.enable_encryption ? 1 : 0
  description             = "KMS key for data lake encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = merge(
    var.tags,
    {
      Purpose = "Data Encryption"
    }
  )
}

resource "aws_kms_alias" "data_lake_key_alias" {
  count         = var.enable_encryption ? 1 : 0
  name          = "alias/data-lake-key-${var.environment}"
  target_key_id = aws_kms_key.data_lake_key[0].key_id
}

############################################
# S3 Buckets: Bronze / Silver / Gold
############################################

# Bronze / RAW
resource "aws_s3_bucket" "raw_data" {
  bucket = "${var.project_prefix}-raw-data-${var.environment}"

  tags = merge(
    var.tags,
    {
      DataLayer = "bronze"
      Retention = "${var.retention_days_raw} days"
    }
  )
}

resource "aws_s3_bucket_versioning" "raw_data" {
  count  = var.enable_versioning ? 1 : 0
  bucket = aws_s3_bucket.raw_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "raw_data_lifecycle"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = var.retention_days_raw
    }
  }
}

# Silver / PROCESSED
resource "aws_s3_bucket" "processed_data" {
  bucket = "${var.project_prefix}-processed-data-${var.environment}"

  tags = merge(
    var.tags,
    {
      DataLayer = "silver"
      Retention = "${var.retention_days_processed} days"
    }
  )
}

resource "aws_s3_bucket_versioning" "processed_data" {
  count  = var.enable_versioning ? 1 : 0
  bucket = aws_s3_bucket.processed_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id

  rule {
    id     = "processed_data_lifecycle"
    status = "Enabled"

    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 180
      storage_class = "GLACIER"
    }

    expiration {
      days = var.retention_days_processed
    }
  }
}

# Gold / CURATED
resource "aws_s3_bucket" "curated_data" {
  bucket = "${var.project_prefix}-curated-data-${var.environment}"

  tags = merge(
    var.tags,
    {
      DataLayer = "gold"
      Retention = "${var.retention_days_curated} days"
    }
  )
}

resource "aws_s3_bucket_versioning" "curated_data" {
  count  = var.enable_versioning ? 1 : 0
  bucket = aws_s3_bucket.curated_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "curated_data" {
  bucket = aws_s3_bucket.curated_data.id

  rule {
    id     = "curated_data_lifecycle"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER"
    }

    expiration {
      days = var.retention_days_curated
    }
  }
}

############################################
# Optional: Default encryption (KMS) on buckets
############################################

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_encryption" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.raw_data.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.data_lake_key[0].arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed_encryption" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.processed_data.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.data_lake_key[0].arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "curated_encryption" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.curated_data.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.data_lake_key[0].arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs_encryption" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.logs.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.data_lake_key[0].arn
      sse_algorithm     = "aws:kms"
    }
  }
}

############################################
# S3 Access logging (raw bucket logs -> logs bucket)
############################################

resource "aws_s3_bucket_logging" "raw_logging" {
  bucket        = aws_s3_bucket.raw_data.id
  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "s3-access/raw/"
}

############################################
# IAM Role/Policy for Glue
############################################

resource "aws_iam_role" "glue_execution_role" {
  name = "GlueExecutionRole-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(var.tags, { Service = "Glue" })
}

resource "aws_iam_policy" "glue_s3_access" {
  name        = "GlueS3Access-${var.environment}"
  description = "Allows Glue to read/write from data lake buckets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*",
          aws_s3_bucket.processed_data.arn,
          "${aws_s3_bucket.processed_data.arn}/*",
          aws_s3_bucket.curated_data.arn,
          "${aws_s3_bucket.curated_data.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_execution_role.name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

############################################
# Glue Catalog + Crawler
############################################

resource "aws_glue_catalog_database" "data_lake" {
  name = "${var.project_prefix}_data_lake_${var.environment}"

  parameters = {
    description = "Central data catalog for analytics"
  }
}

resource "aws_glue_crawler" "raw_crawler" {
  name          = "${var.project_prefix}-raw-crawler-${var.environment}"
  database_name = aws_glue_catalog_database.data_lake.name
  role          = aws_iam_role.glue_execution_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.raw_data.bucket}"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableLevelConfiguration = 2
    }
  })

  schedule = "cron(0 2 * * ? *)" # daily 2 AM

  tags = merge(var.tags, { Schedule = "daily" })
}


# CloudTrail (audit logs -> logs bucket)


resource "aws_cloudtrail" "data_lake_trail" {
  name                          = "${var.project_prefix}-data-lake-trail-${var.environment}"
  s3_bucket_name                = aws_s3_bucket.logs.id
  s3_key_prefix                 = "cloudtrail/"
  include_global_service_events = true
  is_multi_region_trail         = true
  enable_log_file_validation    = true

  advanced_event_selector {
    name = "DataLakeEvents"

    field_selector {
      field  = "eventCategory"
      equals = ["Data"]
    }

    field_selector {
      field  = "resources.type"
      equals = ["AWS::S3::Object"]
    }
  }

  tags = merge(var.tags, { Compliance = "Enabled" })
}
