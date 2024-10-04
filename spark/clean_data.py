import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, regexp_extract, when, date_format, to_date
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType
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
           .withColumn("publish", date_format(col("publish"), "dd/MM/yyyy")) \
           .withColumn("ratingcount", regexp_replace(col("ratingcount"), ",", "")) \
           .withColumn("reviews", regexp_replace(col("reviews"), ",", "")) \
           .drop("pages")

    return df

# Convert data types
def convert(df):
    df = df.withColumn("pages_n", df["pages_n"].cast("int")) \
           .withColumn("prices", df["prices"].cast("float")) \
           .withColumn("onestar", df["onestar"].cast("int")) \
           .withColumn("twostars", df["twostars"].cast("int")) \
           .withColumn("threestars", df["threestars"].cast("int")) \
           .withColumn("fourstars", df["fourstars"].cast("int")) \
           .withColumn("fivestars", df["fivestars"].cast("int")) \
           .withColumn("publish", df["publish"].cast("date")) \
           .withColumn("rating", df["rating"].cast("float")) \
           .withColumn("ratingcount", df["ratingcount"].cast("int")) \
           .withColumn("reviews", df["reviews"].cast("int"))

    return df

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
        
        create_authors_table = """
        CREATE TABLE IF NOT EXISTS authors (
            author_id SERIAL PRIMARY KEY,
            author_name VARCHAR(255) UNIQUE
        );
        """
        
        create_ratings_table = """
        CREATE TABLE IF NOT EXISTS ratings (
            rating_id SERIAL PRIMARY KEY,
            rating FLOAT,
            fivestars INT,
            fourstars INT,
            threestars INT,
            twostars INT,
            onestar INT
        );
        """
        
        create_books_table = """
        CREATE TABLE IF NOT EXISTS books (
            book_id SERIAL PRIMARY KEY,
            rating_id INT,
            author_id INT,
            bookname VARCHAR(255),
            publish DATE,
            prices FLOAT,
            rating FLOAT,
            rating_count INT,
            reviews INT,
            pages_n INT,
            cover VARCHAR(50),
            bookUrl VARCHAR(255),
            FOREIGN KEY (author_id) REFERENCES authors(author_id),
            FOREIGN KEY (rating_id) REFERENCES ratings(rating_id)
        );
        """
        
        create_details_table = """
        CREATE TABLE IF NOT EXISTS details (
            book_id INT PRIMARY KEY,
            author_id INT,
            describe TEXT,
            book_title VARCHAR(255),
            FOREIGN KEY (book_id) REFERENCES books(book_id),
            FOREIGN KEY (author_id) REFERENCES authors(author_id)
        );
        """
        
        cur.execute(create_authors_table)
        cur.execute(create_ratings_table)
        cur.execute(create_books_table)
        cur.execute(create_details_table)
        conn.commit()

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
    query = df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, batch_id: write_batch_to_postgres(batch_df)) \
        .start()

    query.awaitTermination()

# Function to write each batch to PostgreSQL
def write_batch_to_postgres(batch_df):
    rows_books = batch_df.select("bookname", "bookUrl", "author").distinct().collect()
    rows_authors = batch_df.select("author").distinct().collect()
    rows_details = batch_df.select("publish", "pages_n", "cover", "prices").distinct().collect()
    rows_ratings = batch_df.select("rating", "ratingcount", "reviews", "onestar", "twostars", "threestars", "fourstars", "fivestars").distinct().collect()

    # Use context manager for psycopg2
    with psycopg2.connect(
        dbname='goodread',
        user='admin',
        password='admin',
        host='localhost',
        port='5432'
    ) as conn:
        with conn.cursor() as cur:
            try:
                # Insert authors and get their IDs
                author_ids = insert_authors(cur, rows_authors)
                book_ids = insert_books(cur, rows_books, author_ids)
                insert_details(cur, rows_details, book_ids)
                insert_ratings(cur, rows_ratings, book_ids)

                conn.commit()
                print("Data has been inserted successfully.")
                
            except Exception as e:
                print(f"Error occurred while inserting data: {e}")
                conn.rollback()

def insert_authors(cur, rows_authors):
    author_ids = []
    for author in rows_authors:
        cur.execute("INSERT INTO authors (author_name) VALUES (%s) ON CONFLICT (author_name) DO NOTHING RETURNING author_id;", (author[0],))
        author_id = cur.fetchone()
        if author_id:
            author_ids.append(author_id[0])
    return author_ids

def insert_books(cur, rows_books, author_ids):
    book_ids = []
    for book in rows_books:
        author_id = author_ids[0]  # Assume the first author matches (you may adjust logic)
        cur.execute("INSERT INTO books (bookname, bookUrl, author_id) VALUES (%s, %s, %s) ON CONFLICT (bookname) DO NOTHING RETURNING book_id;", (book[0], book[1], author_id))
        book_id = cur.fetchone()
        if book_id:
            book_ids.append(book_id[0])
    return book_ids

def insert_details(cur, rows_details, book_ids):
    for i, detail in enumerate(rows_details):
        cur.execute("INSERT INTO details (book_id, publish, pages_n, cover) VALUES (%s, %s, %s, %s) ON CONFLICT (book_id) DO NOTHING;", (book_ids[i], detail[0], detail[1], detail[2]))

def insert_ratings(cur, rows_ratings, book_ids):
    for i, rating in enumerate(rows_ratings):
        cur.execute("INSERT INTO ratings (rating, rating_count, reviews, onestar, twostars, threestars, fourstars, fivestars) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING rating_id;", 
                    (rating[0], rating[1], rating[2], rating[3], rating[4], rating[5], rating[6], rating[7]))
        rating_id = cur.fetchone()
        if rating_id:
            cur.execute("UPDATE books SET rating_id = %s WHERE book_id = %s;", (rating_id[0], book_ids[i]))

if __name__ == "__main__":
    create_database()
    create_tables()
    spark = connect_kafka()
    df = load_data(spark)
    formatted_df = format_data(df)
#   converted_df = convert(formatted_df)
    insert_data(formatted_df)
    spark.stop()