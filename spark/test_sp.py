import re
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import  col
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName('SparkKafkaToPostgres') \
    .config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0,"
                                    "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()


# Read data from Kafka topic
mongo_df = spark.read.format("mongo").option("uri", "mongodb://localhost:27020") \
        .option("database", "db_goodread") \
        .option("collection", "tb_book") \
        .load()

# Convert the JSON data to columns


# Data cleaning
cleaned_df = mongo_df.withColumn("book_id", F.regexp_extract("bookUrl", r'book/show/(\d+)', 1).cast(IntegerType())) \
    .withColumn("author_id", F.regexp_extract("authorUrl", r'author/show/(\d+)', 1).cast(IntegerType())) \
    .withColumn("prices", F.regexp_extract("prices", r'\$(\d+\.\d+)', 1).cast(FloatType())) \
    .withColumn("rating", col("rating").cast(FloatType())) \
    .withColumn("ratingcount", F.regexp_replace("ratingcount", ',', '').cast(IntegerType())) \
    .withColumn("reviews", F.regexp_replace("reviews", ',', '').cast(IntegerType())) \
    .withColumn("fivestars", F.regexp_replace(col("fivestars"), r"[^\d]", "").cast(IntegerType())) \
    .withColumn("fourstars", F.regexp_replace(col("fourstars"), r"[^\d]", "").cast(IntegerType())) \
    .withColumn("threestars", F.regexp_replace(col("threestars"), r"[^\d]", "").cast(IntegerType())) \
    .withColumn("twostars", F.regexp_replace(col("twostars"), r"[^\d]", "").cast(IntegerType())) \
    .withColumn("onestar", F.regexp_replace(col("onestar"), r"[^\d]", "").cast(IntegerType())) \
    .withColumn("pages_n", F.regexp_extract("pages", r'(\d+)', 1).cast(IntegerType())) \
    .withColumn("cover", F.regexp_extract("pages", r',\s*(\w+)$', 1)) \
    .withColumn("publish", F.to_date(F.regexp_extract(col("publish"), r'(\w+ \d{1,2}, \d{4})', 1), "MMMM d, yyyy")) \
    .drop("pages")

# Split data for different tables
# Split data for different tables
author_df = cleaned_df.select("author_id", "author").distinct()
# author_df = author_df.withColumnRenamed("author", "author_name")

book_df = cleaned_df.select("book_id", "bookname", "author_id", "prices", "describe", "pages_n", "cover", "publish")
# book_df = book_df.withColumnRenamed("describe", "description")

ratings_df = cleaned_df.select("book_id", "rating", "fivestars", "fourstars", "threestars", "twostars", "onestar")
describe_df = cleaned_df.select("book_id", "author_id", "describe", "bookUrl").distinct()

# Define the function to write each micro-batch to PostgreSQL
def write_to_postgres(df, table_name):
    db_properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }
    db_url = "jdbc:postgresql://localhost:5432/goodread"

    # Write the DataFrame to PostgreSQL
    df.write.jdbc(url=db_url, table=table_name, mode="append", properties=db_properties)

# Write each DataFrame to PostgreSQL in the correct order
write_to_postgres(author_df, "Author")       # Insert data into Author table
write_to_postgres(ratings_df, "Rating")      # Insert data into Rating table
write_to_postgres(book_df, "Book")           # Insert data into Book table
write_to_postgres(describe_df, "Describe")   # Insert data into Describe table