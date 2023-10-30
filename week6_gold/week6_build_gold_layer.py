import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
)

# Load environment variables.
from dotenv import load_dotenv

load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


# Create a SparkSession
spark = (
    SparkSession.builder.appName("Week6Lab")
    .config("spark.sql.shuffle.partitions", "3")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375,io.delta:delta-core_2.12:1.0.1",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .master("local[*]")
    .getOrCreate()
)

# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(
    logger.Level.OFF
)
logger.LogManager.getLogger("org.apache.spark.SparkEnv").setLevel(logger.Level.ERROR)

# Define a Schema which describes the Parquet files under the silver reviews directory on S3
silver_fields = [
    ("marketplace", StringType()),
    ("customer_id", StringType()),
    ("review_id", StringType()),
    ("product_id", StringType()),
    ("product_parent", StringType()),
    ("product_title", StringType()),
    ("product_category", StringType()),
    ("star_rating", IntegerType()),
    ("helpful_votes", IntegerType()),
    ("total_votes", IntegerType()),
    ("vine", StringType()),
    ("verified_purchase", StringType()),
    ("review_headline", StringType()),
    ("review_body", StringType()),
    ("purchase_date", StringType()),
    ("review_timestamp", TimestampType()),
    ("customer_name", StringType()),
    ("gender", StringType()),
    ("date_of_birth", StringType()),
    ("city", StringType()),
    ("state", StringType()),
]
silver_schema = StructType([StructField(*field) for field in silver_fields])

# Define a streaming dataframe using readStream on top of the silver reviews directory on S3
silver_data = spark.readStream.schema(silver_schema).parquet(
    "s3a://hwe-fall-2023/mfrench/silver/reviews"
)

# Define a watermarked_data dataframe by defining a watermark on the `review_timestamp` column with an interval of 10 seconds
watermarked_data = silver_data.withWatermark("review_timestamp", "10 seconds")

# Define an aggregated dataframe using `groupBy` functionality to summarize that data over any dimensions you may find interesting
aggregated_data = watermarked_data.groupBy(
    ["review_timestamp", "marketplace", "customer_id", "review_id"]
).agg(
    count("review_id").alias("review_count"),
    sum("helpful_votes").alias("helpful_votes"),
    sum("total_votes").alias("total_votes"),
)

customer_data = watermarked_data.select(
    "customer_id", "customer_name", "gender", "date_of_birth", "city", "state"
).distinct()

# Write that aggregate data to S3 under s3a://hwe-$CLASS/$HANDLE/gold/fact_review using append mode and a checkpoint location of `/tmp/gold-checkpoint`
write_gold_query = (
    aggregated_data.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/gold-checkpoint")
    .option("path", "s3a://hwe-fall-2023/mfrench/gold/fact_review")
)

write_gold_query.start().awaitTermination()

write_customer_query = (
    customer_data.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/gold-checkpoint")
    .option("path", "s3a://hwe-fall-2023/mfrench/gold/dim_customer")
)

write_customer_query.start().awaitTermination()

## Stop the SparkSession
spark.stop()
