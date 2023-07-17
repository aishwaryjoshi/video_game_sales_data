import boto3
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the S3 bucket details
s3_bucket = "aishwary-test-bucket"
s3_key = "indigg-assignment-bucket/output/merged_df.csv/part-00000-0ed0d044-2307-4695-bd07-4a7e6d915b41-c000.csv"

# Set up the S3 client
s3_client = boto3.client("s3")
# Download the CSV file from S3 to a local temporary file
temp_file = "temp_file.csv"
s3_client.download_file(s3_bucket, s3_key, temp_file)

# Read the CSV file into a PySpark DataFrame
df = spark.read.csv(temp_file, header=True, inferSchema=True)
# df.show()

genre_counts = df.groupBy("genre").count()
genre_counts.show()
