import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import current_timestamp
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
    SparkSession.builder.appName("Week5Lab")
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
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375",
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

bronze_schema = StructType(
    [
        StructField("marketplace", StringType, nullable=True),
        StructField("customer_id", StringType, nullable=True),
        StructField("review_id", StringType, nullable=True),
        StructField("product_id", StringType, nullable=True),
        StructField("product_parent", StringType, nullable=True),
        StructField("product_title", StringType, nullable=True),
        StructField("product_category", StringType, nullable=True),
        StructField("star_rating", IntegerType, nullable=True),
        StructField("helpful_votes", IntegerType, nullable=True),
        StructField("total_votes", IntegerType, nullable=True),
        StructField("vine", StringType, nullable=True),
        StructField("verified_purchase", StringType, nullable=True),
        StructField("review_headline", StringType, nullable=True),
        StructField("review_body", StringType, nullable=True),
        StructField("purchase_date", TimestampType, nullable=True),
        StructField("review_timestamp", TimestampType, nullable=True),
    ]
)

bronze_reviews = spark.readStream.schema(bronze_schema).parquet(
    "s3://hwe-fall-2023/mfrench/bronze/reviews"
)

bronze_reviews.createOrReplaceTempView("bronze_reviews")

bronze_customers = spark.read.parquet("s3://hwe-fall-2023/mfrench/bronze/customers")
bronze_customers.createOrReplaceTempView("bronze_customers")

silver_data = spark.sql(
    "SELECT * FROM bronze_reviews JOIN bronze_customers ON bronze_reviews.customer_id = bronze_customers.customer_id"
)

streaming_query = (
    silver_data.writeStream.outputMode("append")
    .format("parquet")
    .option("path", "s3a://hwe-fall-2023/mfrench/silver/reviews")
    .option("checkpointLocation", "/tmp/silver-checkpoint")
    .start()
)

streaming_query.start().awaitTermination()

## Stop the SparkSession
spark.stop()
