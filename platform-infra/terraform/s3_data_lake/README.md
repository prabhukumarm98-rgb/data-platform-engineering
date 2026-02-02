## Usage
```hcl
module "data_lake" {
  source = "./modules/s3_data_lake"
  
  environment = "prod"
  project_prefix = "company-analytics"
  account_id = "123456789012"
  
  retention_days_raw = 365
  retention_days_processed = 180
  retention_days_curated = 90
  
  tags = {
    Department = "Data Engineering"
    CostCenter = "12345"
  }
}

Deployment Commands
# Initialize Terraform
terraform init

# Plan deployment
terraform plan -var-file="production.tfvars"

# Apply configuration
terraform apply -var-file="production.tfvars" -auto-approve

Production Architecture

Raw Data (Bronze) → Processed Data (Silver) → Curated Data (Gold)
      ↓                    ↓                       ↓
   S3 Raw              S3 Processed           S3 Curated
      ↓                    ↓                       ↓
   Glue Crawler       Data Validation       Business Views
      ↓                    ↓                       ↓
   Data Catalog       Quality Metrics       Analytics Ready


### **Step 3: Commit the fix**
- **Commit message:** `fix: correct terraform code syntax in README`
- Click **"Commit changes"**

---

## **WHAT WAS WRONG:**
1. You had:
   ```hcl
   }
   }
Production Architecture
