# Production Data Quality Framework

## Overview
Real-world data validation framework used daily on 500+ production tables. 
Catches data issues before they impact business decisions.

## Features
- **NULL validation** for critical columns
- **Range checking** for numerical data
- **Foreign key integrity** validation
- **Data freshness** monitoring
- **Row count verification** against SLAs

## Usage Example
```python
# Initialize validator
validator = DataValidator(spark_session)

# Check for NULLs in critical columns
result = validator.validate_not_null(df, "customer_id")

# Validate value ranges
validator.validate_value_range(df, "transaction_amount", 0, 1000000)

# Run full validation suite
results = validator.run_full_validation(df, "daily_sales")
