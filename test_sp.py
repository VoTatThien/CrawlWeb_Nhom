import re
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, StructType, StructField, StringType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
        .appName('SparkKafkaToPostgres') \
        .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3," 
                                        "org.postgresql:postgresql:42.5.0") \
        .getOrCreate()

# Define the schema for the data in Kafka messages
schema = StructType([
    StructField("author", StringType(), True),
    StructField("bookUrl", StringType(), True),
    StructField("authorUrl", StringType(), True),
    StructField("bookname", StringType(), True),
    StructField("describe", StringType(), True),
    StructField("prices", StringType(), True),
    StructField("publish", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("ratingcount", StringType(), True),
    StructField("reviews", StringType(), True),
    StructField("fivestars", StringType(), True),
    StructField("fourstars", StringType(), True),
    StructField("threestars", StringType(), True),
    StructField("twostars", StringType(), True),
    StructField("onestar", StringType(), True),
    StructField("pages", StringType(), True)
])

# Read data from Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "goodread") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the Kafka value (which is in bytes) to string and parse it as JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# Data cleaning
cleaned_df = parsed_df.withColumn("book_id", F.regexp_extract("bookUrl", r'book/show/(\d+)', 1).cast(IntegerType())) \
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
    .drop("pages", "bookUrl", "authorUrl")

# Define the function to write each micro-batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    db_properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }
    db_url = "jdbc:postgresql://localhost:5432/goodread"
    table_name = "book_details"
    
    # Write the DataFrame to PostgreSQL
    batch_df.write.jdbc(url=db_url, table=table_name, mode="append", properties=db_properties)

# Write the streaming DataFrame to PostgreSQL using foreachBatch
query = cleaned_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()
