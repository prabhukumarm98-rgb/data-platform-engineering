"""
Production Delta Lake Operations
Manages ACID transactions, time travel, and schema evolution
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime


class DeltaLakeManager:
    """
    Production Delta Lake manager for:
    - ACID transactions
    - Time travel queries
    - Schema evolution
    - Performance optimization
    """
    
    def __init__(self, spark):
        self.spark = spark
        self.base_path = "s3a://data-lake/delta"
        
    def create_delta_table(self, table_name, schema, partition_cols=None):
        """
        Create Delta table with production best practices
        """
        path = f"{self.base_path}/{table_name}"
        
        # Create empty Delta table
        self.spark.createDataFrame([], schema) \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy(partition_cols if partition_cols else []) \
            .save(path)
        
        # Register in Hive metastore
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING DELTA
            LOCATION '{path}'
        """)
        
        print(f"Created Delta table: {table_name} at {path}")
        return DeltaTable.forPath(self.spark, path)
    
    def upsert_data(self, table_name, updates_df, merge_key):
        """
        Production UPSERT (MERGE) operation
        Used for CDC (Change Data Capture) patterns
        """
        delta_table = DeltaTable.forName(self.spark, table_name)
        
        print(f"Starting MERGE on {table_name}")
        print(f"Updates: {updates_df.count()} rows")
        print(f"Merge key: {merge_key}")
        
        # Perform MERGE operation
        merge_condition = " AND ".join(
            [f"target.{col} = source.{col}" for col in merge_key]
        )
        
        delta_table.alias("target").merge(
            updates_df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        
        print(f"MERGE completed on {table_name}")
        
        # Show merge statistics
        self._show_table_stats(table_name)
    
    def time_travel_query(self, table_name, version=None, timestamp=None):
        """
        Query historical versions of Delta table
        """
        delta_table = DeltaTable.forName(self.spark, table_name)
        
        if version:
            df = self.spark.read \
                .format("delta") \
                .option("versionAsOf", version) \
                .table(table_name)
            print(f"Time travel to version {version}")
        elif timestamp:
            df = self.spark.read \
                .format("delta") \
                .option("timestampAsOf", timestamp) \
                .table(table_name)
            print(f"Time travel to timestamp {timestamp}")
        else:
            df = self.spark.table(table_name)
            print(f"Current version of {table_name}")
        
        return df
    
    def schema_evolution(self, table_name, new_columns):
        """
        Evolve schema by adding new columns
        Supports backwards compatibility
        """
        delta_table = DeltaTable.forName(self.spark, table_name)
        
        # Get current schema
        current_schema = delta_table.toDF().schema
        
        print(f"Current schema for {table_name}:")
        for field in current_schema:
            print(f"  - {field.name}: {field.dataType}")
        
        # Add new columns
        alter_queries = []
        for col_name, col_type in new_columns.items():
            alter_queries.append(f"ADD COLUMN {col_name} {col_type}")
        
        # Apply schema evolution
        for query in alter_queries:
            self.spark.sql(f"ALTER TABLE {table_name} {query}")
            print(f"Added column: {query}")
        
        print(f"New schema for {table_name}:")
        new_schema = delta_table.toDF().schema
        for field in new_schema:
            print(f"  - {field.name}: {field.dataType}")
    
    def compact_table(self, table_name):
        """
        Optimize Delta table by compacting small files
        Reduces query time and storage cost
        """
        print(f"Compacting {table_name}...")
        
        # Optimize table (compact small files)
        self.spark.sql(f"""
            OPTIMIZE {table_name}
            ZORDER BY (partition_date, customer_id)
        """)
        
        # Get optimization metrics
        metrics_df = self.spark.sql(f"""
            SELECT 
                operation,
                metrics.numFilesBefore,
                metrics.numFilesAfter,
                metrics.filesAdded,
                metrics.filesRemoved,
                metrics.partitionsOptimized,
                metrics.zOrderStats
            FROM (
                DESCRIBE HISTORY {table_name}
            )
            WHERE operation = 'OPTIMIZE'
            ORDER BY version DESC
            LIMIT 1
        """)
        
        if metrics_df.count() > 0:
            metrics = metrics_df.collect()[0]
            print(f"Compaction completed:")
            print(f"   Files before: {metrics['numFilesBefore']}")
            print(f"   Files after: {metrics['numFilesAfter']}")
            print(f"   Reduction: {metrics['numFilesBefore'] - metrics['numFilesAfter']} files")
        
        # Vacuum old versions
        self.spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")  # Keep 7 days
        print(f"Vacuum completed (kept 7 days of history)")
    
    def show_table_history(self, table_name, limit=10):
        """
        Display table version history
        """
        history_df = self.spark.sql(f"""
            SELECT 
                version,
                timestamp,
                operation,
                operationParameters,
                readVersion,
                isolationLevel,
                isBlindAppend,
                operationMetrics
            FROM (
                DESCRIBE HISTORY {table_name}
            )
            ORDER BY version DESC
            LIMIT {limit}
        """)
        
        print(f"History for {table_name}:")
        history_df.show(truncate=False)
        return history_df
    
    def create_change_data_feed(self, table_name):
        """
        Enable Change Data Feed for CDC patterns
        """
        self.spark.sql(f"""
            ALTER TABLE {table_name}
            SET TBLPROPERTIES (
                delta.enableChangeDataFeed = true
            )
        """)
        print(f"Enabled Change Data Feed for {table_name}")
    
    def query_change_data_feed(self, table_name, start_version, end_version):
        """
        Query changes between versions
        """
        changes_df = self.spark.read \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", start_version) \
            .option("endingVersion", end_version) \
            .table(table_name)
        
        print(f"Change Data Feed from v{start_version} to v{end_version}:")
        print(f"   Total changes: {changes_df.count()}")
        
        changes_df.show(10, truncate=False)
        return changes_df
    
    def _show_table_stats(self, table_name):
        """
        Display table statistics
        """
        stats = self.spark.sql(f"""
            SELECT 
                COUNT(*) as row_count,
                COUNT(DISTINCT customer_id) as unique_customers,
                MIN(transaction_date) as earliest_date,
                MAX(transaction_date) as latest_date,
                SUM(amount) as total_amount
            FROM {table_name}
        """).collect()[0]
        
        print(f"Table Statistics for {table_name}:")
        print(f"   Rows: {stats['row_count']:,}")
        print(f"   Unique customers: {stats['unique_customers']:,}")
        print(f"   Date range: {stats['earliest_date']} to {stats['latest_date']}")
        print(f"   Total amount: ${stats['total_amount']:,.2f}")
    
    def run_production_example(self):
        """
        Example of production Delta Lake operations
        """
        print("="*60)
        print("PRODUCTION DELTA LAKE OPERATIONS")
        print("="*60)
        
        # 1. Create table
        schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("transaction_id", StringType(), False),
            StructField("amount", DecimalType(10, 2), False),
            StructField("transaction_date", DateType(), False),
            StructField("category", StringType(), False)
        ])
        
        delta_table = self.create_delta_table(
            "transactions",
            schema,
            partition_cols=["transaction_date"]
        )
        
        # 2. Insert initial data
        sample_data = [
            (1, "TX1001", 150.50, datetime.date(2024, 1, 15), "Electronics"),
            (2, "TX1002", 75.25, datetime.date(2024, 1, 15), "Clothing"),
            (3, "TX1003", 200.00, datetime.date(2024, 1, 16), "Electronics"),
        ]
        
        df = self.spark.createDataFrame(sample_data, schema)
        df.write.format("delta").mode("append").saveAsTable("transactions")
        print(f"Inserted {df.count()} initial rows")
        
        # 3. Show history
        self.show_table_history("transactions", limit=3)
        
        # 4. Perform MERGE (UPSERT)
        updates_data = [
            (1, "TX1001", 175.50, datetime.date(2024, 1, 15), "Electronics"),  # Update
            (4, "TX1004", 89.99, datetime.date(2024, 1, 17), "Books"),         # Insert
        ]
        
        updates_df = self.spark.createDataFrame(updates_data, schema)
        self.upsert_data("transactions", updates_df, ["customer_id", "transaction_id"])
        
        # 5. Time travel to before MERGE
        print("\nTime Travel Example:")
        old_version = self.time_travel_query("transactions", version=0)
        print(f"Version 0 had {old_version.count()} rows")
        
        # 6. Schema evolution
        print("\nSchema Evolution:")
        self.schema_evolution("transactions", {
            "payment_method": "STRING",
            "currency": "STRING DEFAULT 'USD'"
        })
        
        # 7. Compact table
        print("\nTable Compaction:")
        self.compact_table("transactions")
        
        print("\n" + "="*60)
        print("All Delta Lake operations completed successfully!")
        print("="*60)


# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DeltaLakeOperations") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    manager = DeltaLakeManager(spark)
    manager.run_production_example()
