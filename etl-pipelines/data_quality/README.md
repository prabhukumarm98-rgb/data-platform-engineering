# Production Data Quality Framework

## Overview
This module demonstrates a **production-grade data quality validation framework**
designed to prevent bad data from propagating into downstream analytics layers.

The framework is intentionally lightweight and focuses on **practical checks used
in real enterprise data pipelines**, especially in Bronze → Silver → Gold patterns.

---

## Why Data Quality Matters
In production environments, data issues such as null values, duplicates, or
unexpected row drops can silently break reports and business metrics.

This framework addresses those risks by enforcing validations **before data
is promoted to curated layers**.

---

## Core Quality Checks

### 1. Null Validation
Ensures critical business columns do not contain null values.
- Example: `order_id`, `customer_id`, `transaction_date`

### 2. Duplicate Detection
Identifies duplicate records based on natural or surrogate keys.

### 3. Row Count Reconciliation
Compares source and target row counts to detect data loss during ingestion.

### 4. Schema Validation
Validates incoming data against expected schema contracts.

---

## Failure Handling Strategy
Each check supports configurable behavior:
- **FAIL** – stop pipeline execution
- **WARN** – log issue and continue processing

This mirrors real-world operational trade-offs between data freshness and accuracy.

---

## Integration Pattern
This module is designed to integrate with:
- PySpark-based ETL pipelines
- Bronze / Silver / Gold architectures
- Observability and monitoring layers

---

## Technology Stack
- PySpark
- Spark SQL
- Python
