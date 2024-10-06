import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, regexp_extract, when, to_date, lit, from_json, round
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType, Row

import psycopg2

# Connect to Kafka
def connect_kafka():
    spark = SparkSession.builder \
        .appName('SparkKafkaToPostgres') \
        .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3," 
                                        "org.postgresql:postgresql:42.5.0") \
        .getOrCreate()
    return spark

# Load data from Kafka
def load_data(spark):
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "goodread") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    return df

# Format data
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

# Format data   
def format_data(df):
    df = df.selectExpr("CAST(value AS STRING)") \
        .selectExpr(f"from_json(value, '{schema.simpleString()}') as jsonData") \
        .select("jsonData.*")
    # Processing string data
    df = df.withColumn("onestar", regexp_replace(col("onestar"), r"[^\d]", "")) \
           .withColumn("twostars", regexp_replace(col("twostars"), r"[^\d]", "")) \
           .withColumn("threestars", regexp_replace(col("threestars"), r"[^\d]", "")) \
           .withColumn("fourstars", regexp_replace(col("fourstars"), r"[^\d]", "")) \
           .withColumn("fivestars", regexp_replace(col("fivestars"), r"[^\d]", "")) \
           .withColumn("pages_n", regexp_extract(col("pages"), r"(\d+)", 1)) \
           .withColumn("cover", regexp_extract(col("pages"), r",\s*(.*)", 1)) \
           .withColumn("prices", when(df["prices"].like("Kindle%"), regexp_extract(col("prices"), r"\$(\d+\.\d{2})", 1)).otherwise(0)) \
           .withColumn("publish", regexp_extract(col("publish"), r"(\w+ \d{1,2}, \d{4})", 1)) \
           .withColumn("publish", to_date(col("publish"), "MMMM d, yyyy")) \
           .withColumn("ratingcount", regexp_replace(col("ratingcount"), ",", "")) \
           .withColumn("reviews", regexp_replace(col("reviews"), ",", "")) \
           .drop("pages")

    return df

# Convert data types
# def convert(df):
#     # Định nghĩa ánh xạ giữa tên cột và kiểu dữ liệu phù hợp với PostgreSQL
#     column_types = {
#         "author": StringType(),
#         "bookUrl": StringType(),
#         "bookname": StringType(),
#         "describe": StringType(),
#         "rating": FloatType(),
#         "ratingcount": IntegerType(),
#         "reviews": IntegerType(),
#         "fivestars": IntegerType(),
#         "fourstars": IntegerType(),
#         "threestars": IntegerType(),
#         "twostars": IntegerType(),
#         "onestar": IntegerType(),
#         "pages_n": IntegerType(),
#         "prices": FloatType(),
#         "publish": DateType()
#     }

#     # Sử dụng vòng lặp để ép kiểu cho từng cột
#     for column, dtype in column_types.items():
#         df = df.withColumn(column, df[column].cast(dtype))

#     return df

# Create PostgreSQL tables based on provided schema
def create_database():
    conn = psycopg2.connect(
        dbname='postgres',  
        user='admin',
        password='admin',
        host='localhost',
        port='5432'
    )
    conn.autocommit = True  
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'goodread'")
        exists = cur.fetchone()
        if not exists:
            cur.execute("CREATE DATABASE goodread;")
            print("Database 'goodread' has been created successfully.")
        else:
            print("Database 'goodread' already exists.")
    except Exception as e:
        print(f"Error occurred while creating database: {e}")
    finally:
        cur.close()
        conn.close()


def create_tables():
    try:
        conn = psycopg2.connect(
            dbname='goodread',
            user='admin',
            password='admin',
            host='localhost',
            port='5432'
        )
        cur = conn.cursor()

    #create table
        create_books_table = """
        CREATE TABLE IF NOT EXISTS Book (
            book_id SERIAL PRIMARY KEY,

        
        # Create authors table
    #     create_authors_table = """
    #     CREATE TABLE IF NOT EXISTS Author (
    #         id_author SERIAL PRIMARY KEY,
    #         author_name VARCHAR(255) 
    #     );
    #     """
        
    #     # Create ratings table
    #     create_ratings_table = """
    #     CREATE TABLE Rating (
    #         rating_id INT PRIMARY KEY,
    #         rating FLOAT,
    #         fivestars INT,
    #         fourstars INT,
    #         threestars INT,
    #         twostars INT,
    #         onestar INT
    #     );
    #     """
        
    #     # Create books table
    #     create_books_table = """
    #     CREATE TABLE Book (
    #     book_id INT PRIMARY KEY,
    #     rating_id INT,
    #     id_author INT,
    #     rating FLOAT,
    #     describe VARCHAR(255),
    #     author_name VARCHAR(255),
    #     bookname VARCHAR(255),
    #     publish VARCHAR(255),
    #     prices FLOAT,
    #     rating_count INT,
    #     reviews INT,
    #     pages_n INT,
    #     cover VARCHAR(255),
    #     bookUrl VARCHAR(255),
    #     fivestars INT,
    #     fourstars INT,
    #     threestars INT,
    #     twostars INT,
    #     onestar INT,
    #     FOREIGN KEY (rating_id) REFERENCES Rating(rating_id),
    #     FOREIGN KEY (id_author) REFERENCES Author(id_author)
    # );
    #     """
        
    #     # Create details table
    #     create_details_table = """
    #     CREATE TABLE IF NOT EXISTS Detail (
    #     book_id INT PRIMARY KEY,
    #     id_author INT,
    #     describe VARCHAR(255),
    #     bookUrl VARCHAR(255),
    #     FOREIGN KEY (book_id) REFERENCES Book(book_id),
    #     FOREIGN KEY (id_author) REFERENCES Author(id_author)
    #     );
    #     """
        
    #     # Execute table creation queries
    #     cur.execute(create_authors_table)
    #     cur.execute(create_ratings_table)
    #     cur.execute(create_books_table)
    #     cur.execute(create_details_table)
    #     conn.commit()

        print("Tables have been created successfully.")
        
    except Exception as e:
        print(f"Error occurred while creating tables: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# Insert data into PostgreSQL
def insert_data(df):
    df.writeStream \
        .foreachBatch(write_to_db) \
        .outputMode("append") \
        .start() \
        .awaitTermination()

# PostgreSQL connection properties
jdbc_url = "jdbc:postgresql://localhost:5432/goodread"
connection_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Function to write each batch to PostgreSQL
def write_to_db(df, epoch_id):
    for row in df.collect():
        book_data = row.asDict()
        insert_rating_data(book_data)
        insert_author_data(book_data)
        insert_book_data(book_data)
        insert_details_data(book_data)

# Function to insert books
def insert_book_data(book_data):
    book_row = Row(**book_data)
    book_df = spark.createDataFrame([book_row])

    book_df_final = book_df.select(
        lit(1).alias("book_id"),
        lit(1).alias("rating_id"),
        lit(1).alias("id_author"),
        book_df.rating.alias("rating"),
        book_df.describe.alias("describe"),
        book_df.author.alias("author_name"),
        book_df.bookname.alias("bookname"),
        book_df.publish.alias("publish"),
        book_df.prices.alias("prices"),
        book_df.ratingcount.alias("rating_count"),
        book_df.reviews.alias("reviews"),
        book_df.pages_n.alias("pages_n"),
        book_df.bookUrl.alias("bookUrl"),
        book_df.fivestars.alias("fivestars"),
        book_df.fourstars.alias("fourstars"),
        book_df.threestars.alias("threestars"),
        book_df.twostars.alias("twostars"),
        book_df.onestar.alias("onestar")
    )

    book_df_final.write.jdbc(url=jdbc_url, table="Book", mode="append", properties=connection_properties)

# Function to insert authors

def insert_author_data(book_data):
    book_row = Row(**book_data)
    book_df = spark.createDataFrame([book_row])

    author_df = book_df.select(
        lit(1).alias("id_author"),
        book_df.author.alias("author_name")
    )

    author_df.write.jdbc(url=jdbc_url, table="Author", mode="append", properties=connection_properties)


# Function to insert book details
def insert_details_data(book_data):
    book_row = Row(**book_data)
    book_df = spark.createDataFrame([book_row])

    describe_df = book_df.select(
        lit(1).alias("book_id"),
        lit(1).alias("id_author"),
        book_df.describe.alias("describe"),
        book_df.bookUrl.alias("bookUrl")
    )

    describe_df.write.jdbc(url=jdbc_url, table="Detail", mode="append", properties=connection_properties)

# Function to insert ratings
def insert_rating_data(book_data):
    book_row = Row(**book_data)
    book_df = spark.createDataFrame([book_row])

    rating_df = book_df.select(
        lit(1).alias("rating_id"),
        book_df.rating.alias("rating"),
        book_df.fivestars.alias("fivestars"),
        book_df.fourstars.alias("fourstars"),
        book_df.threestars.alias("threestars"),
        book_df.twostars.alias("twostars"),
        book_df.onestar.alias("onestar")
    )

    rating_df.write.jdbc(url=jdbc_url, table="Rating", mode="append", properties=connection_properties)


if __name__ == "__main__":
    create_database()
    spark = connect_kafka()
    df = load_data(spark)
    db_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
    }
    db_url = "jdbc:postgresql://localhost:5432/goodread"

# Table name
    table_name = "book_details"

# Write DataFrame to PostgreSQL
    df.writeStream .jdbc(url=db_url, table=table_name, mode="overwrite", properties=db_properties)

# Stop Spark session
    spark.stop()
