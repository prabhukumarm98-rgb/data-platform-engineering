# Production Data Quality Framework

## Overview
This module implements a **production-grade data quality validation framework**
used to validate **500+ tables daily** in a data warehouse environment.

It is designed to catch **data integrity, freshness, and volume issues**
_before_ data reaches analytics, dashboards, or downstream consumers.

The framework reflects **real-world enterprise data engineering practices**,
including configurable severity levels, logging, and monitoring-friendly outputs.

---

## Key Capabilities

### 1. Null Validation
Detects NULL values in critical business columns and classifies issues based on
production thresholds.

- PASS: Acceptable null percentage
- WARNING: Moderate data quality degradation
- FAIL: Critical data integrity issue

---

### 2. Value Range Validation
Ensures numerical columns fall within expected business ranges.

**Production use cases:**
- Transaction amounts
- Age fields
- Quantitative metrics

Out-of-range values are logged with sample records for debugging.

---

### 3. Foreign Key Integrity Checks
Validates referential integrity between fact and dimension tables.

- Identifies orphaned foreign keys
- Persists invalid keys for offline investigation
- Prevents silent data corruption in analytics layers

---

### 4. Row Count Validation
Validates daily data volumes against expected thresholds.

**Used for:**
- Detecting ingestion failures
- Identifying duplicate loads
- SLA compliance checks

---

### 5. Data Freshness Validation
Ensures datasets meet freshness SLAs required for dashboards and reporting.

- Compares latest available timestamp against configurable thresholds
- Critical for near-real-time and operational reporting

---

## Failure Handling Strategy

Each validation produces one of three outcomes:

| Status   | Meaning |
|--------|--------|
| PASS   | Data meets quality expectations |
| WARNING | Data issue detected but pipeline may continue |
| FAIL   | Critical issue – pipeline should stop |

This mirrors real-world trade-offs between **data accuracy and availability**.

---

## Logging & Observability

- INFO logs for successful checks
- WARNING logs for non-blocking issues
- ERROR logs for critical failures
- Persistent log file for production monitoring

Designed to integrate with centralized logging and alerting systems.

---

## Example Usage

```python
validator = DataValidator(spark)

validator.validate_not_null(df, "customer_id")
validator.validate_value_range(df, "age", 0, 120)
validator.validate_row_count(df, expected_min=1000)
validator.validate_date_freshness(df, "transaction_date")

results = validator.run_full_validation(df, "customer_transactions")
