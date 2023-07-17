from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the CSV file into a PySpark DataFrame
csv_file = "temp_file.csv"  # Specify the CSV file name
df = spark.read.csv(csv_file, header=True, inferSchema=True)

# Display the DataFrame
df.show()
df.printSchema()

# Group by 'genre' column and count rows
genre_counts = df.groupBy("genre").count().orderBy("genre")

# Sort the genre counts in alphabetical order of genre
# genre_counts = genre_counts.orderBy(col("genre").asc())
# Display the genre counts
genre_counts.show()


# s3_bucket = "s3://aishwary-test-bucket/indigg-assignment-bucket/output/merged_df.csv/"
# genre_counts.write.csv(s3_bucket, header=True, mode="overwrite")
