from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col

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
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()
