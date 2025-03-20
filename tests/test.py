import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SHA2Example") \
    .getOrCreate()

# Define sample data
data = [
    Row(CustomerID="C001", Name="John Doe", Email="john.doe@example.com"),
    Row(CustomerID="C002", Name="Jane Smith", Email="jane.smith@example.com")
]

# Create DataFrame
dataframe = spark.createDataFrame(data)

# Compute SHA-256 hash for the 'Email' column
dataframe_with_hash = dataframe.withColumn("Email_Hash", F.sha2(F.col("Email"), 256))

# Show the resulting DataFrame
dataframe_with_hash.show(truncate=False)