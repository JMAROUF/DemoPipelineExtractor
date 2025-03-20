from pyspark.sql import SparkSession
from delta import *
import pyspark.sql.functions as F

# Initialize Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("GlueDeltaLakeExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read data from a source (e.g., S3)
input_path = "s3://your-bucket/input-data/"
df = spark.read.format("csv").option("header", "true").load(input_path)

# Perform some transformations
df_transformed = df.withColumnRenamed("old_column_name", "new_column_name")

# Add historization columns
df_transformed = df_transformed \
    .withColumn("valid_from", F.current_timestamp()) \
    .withColumn("valid_to", F.lit(None).cast("timestamp")) \
    .withColumn("is_active", F.lit(True))

# Write data to Delta Lake
output_path = "s3://your-bucket/output-data/"
df_transformed.write.format("delta").mode("overwrite").save(output_path)

# Create a Delta table
spark.sql(f"CREATE TABLE IF NOT EXISTS delta_table USING DELTA LOCATION '{output_path}'")

# Function to upsert data with historization
def upsert_data(new_data, delta_table_path):
    delta_table = DeltaTable.forPath(spark, delta_table_path)

    # Mark existing records as inactive
    delta_table.alias("tgt").merge(
        new_data.alias("src"),
        "tgt.id = src.id AND tgt.is_active = true"
    ).whenMatchedUpdate(set={
        "valid_to": F.current_timestamp(),
        "is_active": F.lit(False)
    }).execute()

    # Insert new records
    new_data.withColumn("valid_from", F.current_timestamp()) \
        .withColumn("valid_to", F.lit(None).cast("timestamp")) \
        .withColumn("is_active", F.lit(True)) \
        .write.format("delta").mode("append").save(delta_table_path)

# Example of upserting new data
new_data = spark.read.format("csv").option("header", "true").load("s3://your-bucket/new-data/")
upsert_data(new_data, output_path)

# Read data from Delta Lake
delta_df = spark.read.format("delta").load(output_path)

# Show the data
delta_df.show()

# Stop the Spark session
spark.stop()