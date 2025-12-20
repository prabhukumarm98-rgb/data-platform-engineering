"""
Production Data Quality Validator
Used daily to validate 500+ tables in our data warehouse
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import logging
from dataclasses import dataclass
from enum import Enum


class ValidationResult(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"


@dataclass
class ValidationRule:
    """Rule definition for data validation"""
    name: str
    description: str
    severity: str  # ERROR, WARNING
    sql_check: Optional[str] = None
    python_check: Optional[callable] = None
    threshold: Optional[float] = None


class DataValidator:
    """
    Production data validator used for:
    - Daily data quality checks
    - Pipeline monitoring
    - SLA compliance tracking
    """
    
    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.logger = self._setup_logger()
        self.validation_results = []
        
    def _setup_logger(self):
        """Setup production logging"""
        logger = logging.getLogger('DataValidator')
        logger.setLevel(logging.INFO)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # File handler for production logs
        fh = logging.FileHandler('data_validation.log')
        fh.setLevel(logging.WARNING)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        
        logger.addHandler(ch)
        logger.addHandler(fh)
        return logger
    
    def validate_not_null(self, df, column_name: str) -> ValidationResult:
        """
        Check for NULL values in critical columns
        Used in production for key business columns
        """
        null_count = df.filter(df[column_name].isNull()).count()
        total_count = df.count()
        
        if total_count == 0:
            self.logger.warning(f"Empty dataframe for column {column_name}")
            return ValidationResult.WARNING
        
        null_percentage = (null_count / total_count) * 100
        
        self.logger.info(
            f"NULL check for {column_name}: "
            f"{null_count}/{total_count} nulls ({null_percentage:.2f}%)"
        )
        
        if null_percentage > 5.0:  # Production threshold
            self.logger.error(
                f"High NULL percentage in {column_name}: {null_percentage:.2f}%"
            )
            return ValidationResult.FAIL
        elif null_percentage > 1.0:
            self.logger.warning(
                f"Moderate NULL percentage in {column_name}: {null_percentage:.2f}%"
            )
            return ValidationResult.WARNING
        else:
            self.logger.info(f"NULL check passed for {column_name}")
            return ValidationResult.PASS
    
    def validate_value_range(self, df, column_name: str, 
                           min_val: float, max_val: float) -> ValidationResult:
        """
        Validate numerical columns are within expected ranges
        Production example: transaction amounts, ages, etc.
        """
        out_of_range_count = df.filter(
            (df[column_name] < min_val) | (df[column_name] > max_val)
        ).count()
        
        if out_of_range_count > 0:
            self.logger.error(
                f"{out_of_range_count} rows out of range "
                f"for {column_name} ({min_val}-{max_val})"
            )
            
            # Log sample of bad values for debugging
            bad_samples = df.filter(
                (df[column_name] < min_val) | (df[column_name] > max_val)
            ).select(column_name).limit(5).collect()
            
            sample_values = [row[column_name] for row in bad_samples]
            self.logger.debug(f"Sample bad values: {sample_values}")
            
            return ValidationResult.FAIL
        
        self.logger.info(f"Range check passed for {column_name}")
        return ValidationResult.PASS
    
    def validate_foreign_key(self, df, column_name: str, 
                           ref_df, ref_column: str) -> ValidationResult:
        """
        Validate foreign key relationships
        Critical for data integrity in production
        """
        # Get distinct values from both tables
        df_values = df.select(column_name).distinct()
        ref_values = ref_df.select(ref_column).distinct()
        
        # Find values in df that don't exist in reference
        invalid_values = df_values.join(
            ref_values,
            df_values[column_name] == ref_values[ref_column],
            "left_anti"
        )
        
        invalid_count = invalid_values.count()
        
        if invalid_count > 0:
            self.logger.error(
                f"{invalid_count} invalid foreign key values in {column_name}"
            )
            
            # For production debugging, save invalid values
            if self.spark:
                invalid_values.coalesce(1).write.mode('overwrite').csv(
                    f"invalid_fk_{column_name}_{datetime.now().strftime('%Y%m%d')}"
                )
            
            return ValidationResult.FAIL
        
        self.logger.info(f"Foreign key check passed for {column_name}")
        return ValidationResult.PASS
    
    def validate_row_count(self, df, expected_min: int, 
                          expected_max: Optional[int] = None) -> ValidationResult:
        """
        Validate expected row count ranges
        Used for daily volume checks in production
        """
        actual_count = df.count()
        
        self.logger.info(f"Row count: {actual_count:,} (expected: {expected_min:,}+)")
        
        if actual_count < expected_min:
            self.logger.error(
                f"Insufficient rows: {actual_count:,} < {expected_min:,}"
            )
            return ValidationResult.FAIL
        
        if expected_max and actual_count > expected_max:
            self.logger.error(
                f"Excessive rows: {actual_count:,} > {expected_max:,}"
            )
            return ValidationResult.FAIL
        
        self.logger.info(f"Row count validation passed")
        return ValidationResult.PASS
    
    def validate_date_freshness(self, df, date_column: str, 
                              max_hours_old: int = 24) -> ValidationResult:
        """
        Validate data freshness - critical for real-time dashboards
        """
        if not self.spark:
            self.logger.warning("Spark session required for date freshness check")
            return ValidationResult.WARNING
        
        # Get most recent date
        latest_date = df.agg({date_column: "max"}).collect()[0][0]
        
        if not latest_date:
            self.logger.error(f"No dates found in {date_column}")
            return ValidationResult.FAIL
        
        # Calculate age
        current_time = datetime.now()
        if isinstance(latest_date, str):
            latest_date = datetime.strptime(latest_date, '%Y-%m-%d %H:%M:%S')
        
        age_hours = (current_time - latest_date).total_seconds() / 3600
        
        self.logger.info(
            f"Data freshness: {age_hours:.1f} hours old "
            f"(latest: {latest_date})"
        )
        
        if age_hours > max_hours_old:
            self.logger.error(
                f"Data stale: {age_hours:.1f}h > {max_hours_old}h limit"
            )
            return ValidationResult.FAIL
        
        self.logger.info(f"Data freshness check passed")
        return ValidationResult.PASS
    
    def run_full_validation(self, df, table_name: str) -> Dict:
        """
        Run comprehensive validation suite
        Returns detailed results for monitoring
        """
        self.logger.info(f"Starting validation for {table_name}")
        
        results = {
            'table_name': table_name,
            'validation_time': datetime.now().isoformat(),
            'total_rows': df.count(),
            'checks': [],
            'overall_status': ValidationResult.PASS
        }
        
        # Example validation suite - customize based on table
        checks = [
            ('row_count', self.validate_row_count(df, 1000)),
            ('not_null_id', self.validate_not_null(df, 'id')),
        ]
        
        for check_name, result in checks:
            check_result = {
                'check': check_name,
                'status': result.value,
                'timestamp': datetime.now().isoformat()
            }
            results['checks'].append(check_result)
            
            if result == ValidationResult.FAIL:
                results['overall_status'] = ValidationResult.FAIL
        
        self.logger.info(
            f"Validation complete for {table_name}: "
            f"{results['overall_status'].value}"
        )
        
        return results


# Example usage in production
def example_usage():
    """Show how this validator is used in real pipelines"""
    
    # Initialize Spark (in production)
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("DataQualityValidation") \
        .getOrCreate()
    
    # Sample data
    data = [
        (1, "John", 25, "2024-01-15", 100.50),
        (2, "Jane", 30, "2024-01-15", 200.75),
        (3, None, 35, "2024-01-14", 150.00),  # NULL name
        (4, "Bob", 150, "2024-01-15", 500.00),  # Invalid age
        (5, "Alice", 28, "2024-01-15", 300.25),
    ]
    
    df = spark.createDataFrame(
        data, 
        ["id", "name", "age", "transaction_date", "amount"]
    )
    
    # Initialize validator
    validator = DataValidator(spark)
    
    print("="*60)
    print("PRODUCTION DATA VALIDATION EXAMPLE")
    print("="*60)
    
    # Run validations
    print("\n1. Checking for NULL values in critical columns:")
    result1 = validator.validate_not_null(df, "name")
    print(f"Result: {result1.value}")
    
    print("\n2. Validating age range (0-120):")
    result2 = validator.validate_value_range(df, "age", 0, 120)
    print(f"Result: {result2.value}")
    
    print("\n3. Validating row count:")
    result3 = validator.validate_row_count(df, 3, 10)
    print(f"Result: {result3.value}")
    
    print("\n4. Running full validation suite:")
    full_results = validator.run_full_validation(df, "customer_transactions")
    print(f"Overall Status: {full_results['overall_status'].value}")
    
    print("\n" + "="*60)
    print("Validation complete!")
    print("="*60)


if __name__ == "__main__":
    example_usage()
