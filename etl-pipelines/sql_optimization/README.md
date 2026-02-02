# Production SQL Optimization Examples

## Overview
This module showcases **real-world SQL performance tuning techniques**
applied to **10TB+ production datasets**.

The examples are based on actual query optimization work where execution
times were reduced from **45 minutes to ~3 minutes (15x improvement)**.

The focus is on **practical, repeatable optimization patterns** rather than
theoretical SQL tricks.

---

## Problem Context
In large-scale analytical systems, poorly optimized SQL can:
- Trigger full table scans
- Consume excessive compute resources
- Miss SLA requirements
- Impact downstream pipelines and dashboards

This module demonstrates how to **identify bottlenecks** and **systematically
refactor queries** for performance and scalability.

---

## Example 1: Original Slow Query

### Characteristics
- Multiple nested CTEs causing repeated scans
- No effective partition pruning
- Expensive window functions over large datasets
- Unoptimized joins

### Result
- Execution time: **~45 minutes**
- High data scan volume
- Inefficient query plan

This example represents a **common pattern seen in real production systems**.

---

## Example 2: Optimized Version (15x Faster)

### Key Optimizations Applied

#### 1. Partition Pruning
Filtered data early using date-based predicates to reduce scanned data.

```sql
WHERE event_date >= '2024-01-01'
AND event_date < '2024-02-01'
