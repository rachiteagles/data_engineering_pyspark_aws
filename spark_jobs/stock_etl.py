from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, DateType, StringType
from pyspark.sql.functions import col, pivot, first, lag, current_timestamp, lit
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder.appName("StockETL").getOrCreate()

# Define Schema
schema = StructType([
    StructField("date", DateType(), True),
    StructField("id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("trade_volume", IntegerType(), True)
])

# S3 Bucket and Input Path
S3_BUCKET = "your-bucket-name"
S3_PATH = f"s3://{S3_BUCKET}/stocks/"
JDBC_URL = "jdbc:postgresql://your-rds-endpoint:5432/yourdb"
DB_PROPERTIES = {"user": "your-user", "password": "your-password", "driver": "org.postgresql.Driver"}

# Define Schema
schema = StructType([
    StructField("date", DateType(), True),
    StructField("id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("trade_volume", IntegerType(), True)
])

# Load Data from multiple csv files present in S3_PATH
df = spark.read.option("header", True).option("mode", "PERMISSIVE").schema(schema).csv(S3_PATH)
df = df.withColumn("error", lit(None).cast(StringType()))  # Initialize error column

# Pivot Data to Wide Format (Price Table)
price_df = df.groupBy("date").pivot("id").agg(first("price"))
for i in range(1,201):
    price_df = price_df.withColumnRenamed(f"{i}", f"stk_00{i}") # Rename columns
price_df = price_df.withColumn("timestamp", current_timestamp())  # Add timestamp column

# Pivot Data to Wide Format (Volume Table)
volume_df = df.groupBy("date").pivot("id").agg(first("trade_volume"))
for i in range(1,201):
    volume_df = volume_df.withColumnRenamed(f"{i}", f"stk_00{i}") # Rename columns
volume_df = volume_df.withColumn("timestamp", current_timestamp())  # Add timestamp column

# Calculate Returns (Percentage Change). return = (price(t)/price(t-1))-1
window_spec = Window.partitionBy().orderBy("date")
returns_df = price_df.select("date", *[(col(c) / lag(c).over(window_spec) - 1).alias(c) for c in price_df.columns if c != "date"])
returns_df = returns_df.withColumn("timestamp", current_timestamp())  # Add timestamp column

# Save to PostgreSQL
price_df.write.jdbc(url=JDBC_URL, table="price", mode="append", properties=DB_PROPERTIES)
volume_df.write.jdbc(url=JDBC_URL, table="volume", mode="append", properties=DB_PROPERTIES)
returns_df.write.jdbc(url=JDBC_URL, table="returns", mode="append", properties=DB_PROPERTIES)

# Stop Spark Session
spark.stop()
