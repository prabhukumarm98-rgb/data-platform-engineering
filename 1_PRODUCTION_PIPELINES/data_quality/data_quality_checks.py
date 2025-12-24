from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class DataQualityException(Exception):
    """Raised when data quality checks fail"""
    pass


def check_nulls(
    df: DataFrame,
    required_columns: list,
    fail_on_error: bool = True
) -> None:
    """
    Validate that required columns do not contain NULL values
    """
    null_counts = {
        c: df.filter(col(c).isNull()).count()
        for c in required_columns
    }

    failed = {c: cnt for c, cnt in null_counts.items() if cnt > 0}

    if failed:
        message = f"Null check failed for columns: {failed}"
        if fail_on_error:
            raise DataQualityException(message)
        else:
            print(f"WARNING: {message}")


def check_duplicates(
    df: DataFrame,
    key_columns: list,
    fail_on_error: bool = True
) -> None:
    """
    Validate that no duplicate records exist based on key columns
    """
    duplicate_count = (
        df.groupBy(key_columns)
          .count()
          .filter(col("count") > 1)
          .count()
    )

    if duplicate_count > 0:
        message = f"Duplicate records found based on keys: {key_columns}"
        if fail_on_error:
            raise DataQualityException(message)
        else:
            print(f"WARNING: {message}")


def check_row_count(
    source_df: DataFrame,
    target_df: DataFrame,
    tolerance_pct: float = 0.0,
    fail_on_error: bool = True
) -> None:
    """
    Reconcile row counts between source and target datasets
    """
    source_count = source_df.count()
    target_count = target_df.count()

    diff_pct = abs(source_count - target_count) / max(source_count, 1)

    if diff_pct > tolerance_pct:
        message = (
            f"Row count mismatch. "
            f"Source={source_count}, Target={target_count}, Diff%={diff_pct:.2%}"
        )
        if fail_on_error:
            raise DataQualityException(message)
        else:
            print(f"WARNING: {message}")
