# Output values for data lake module

output "data_lake_buckets" {
  description = "Map of data lake bucket names"
  value = {
    raw       = aws_s3_bucket.raw_data.bucket
    processed = aws_s3_bucket.processed_data.bucket
    curated   = aws_s3_bucket.curated_data.bucket
  }
}

output "bucket_arns" {
  description = "ARNs of data lake buckets"
  value = {
    raw       = aws_s3_bucket.raw_data.arn
    processed = aws_s3_bucket.processed_data.arn
    curated   = aws_s3_bucket.curated_data.arn
  }
}

output "glue_resources" {
  description = "Glue catalog resources"
  value = {
    database_name = aws_glue_catalog_database.data_lake.name
    crawler_name  = aws_glue_crawler.raw_crawler.name
    crawler_schedule = aws_glue_crawler.raw_crawler.schedule
  }
}

output "iam_role_arn" {
  description = "ARN of Glue execution IAM role"
  value       = aws_iam_role.glue_execution_role.arn
}

output "security_info" {
  description = "Security-related information"
  value = {
    kms_key_id      = aws_kms_key.data_lake_key.key_id
    kms_key_alias   = aws_kms_alias.data_lake_key_alias.name
    cloudtrail_name = aws_cloudtrail.data_lake_trail.name
  }
  sensitive = true
}

output "monitoring_info" {
  description = "Monitoring and logging information"
  value = {
    s3_logging_enabled   = true
    cloudtrail_enabled   = true
    bucket_versioning    = var.enable_versioning
    server_side_encryption = var.enable_encryption
  }
}
