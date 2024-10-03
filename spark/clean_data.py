import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import regexp_replace, col, split, regexp_extract, when , date_format, to_date

def connect_mongo():
    spark = SparkSession.builder.appName("Spark Dataframe Introduction") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.0") \
        .config("spark.mongodb.connection.uri", "mongodb://localhost:27020/db_goodread") \
        .config("spark.mongodb.database", "db_goodread") \
        .config("spark.mongodb.collection", "tb_book") \
        .getOrCreate()
    return spark    

def load_data(spark):
    df = spark.read.format("mongo") \
        .option("database", "db_goodread") \
        .option("collection", "tb_book") \
        .load()       
    return df

def format_data(df):
    df = df.withColumn("onestar", regexp_replace(col("onestar"), r"[^\d]", ""))
    df = df.withColumn("twostars", regexp_replace(col("twostars"), r"[^\d]", ""))
    df = df.withColumn("threestars", regexp_replace(col("threestars"), r"[^\d]", ""))
    df = df.withColumn("fourstars", regexp_replace(col("fourstars"), r"[^\d]", ""))
    df = df.withColumn("fivestars", regexp_replace(col("fivestars"), r"[^\d]", ""))
    df = df.withColumn("pages_n", regexp_extract(col("pages"), r"(\d+)", 1))
    df = df.withColumn("cover", regexp_extract(col("pages"), r",\s*(.*)", 1))
    df = df.withColumn("prices", when(df["prices"].like("Kindle%"), regexp_extract(col("prices"), r"\$(\d+\.\d{2})", 1)).otherwise(0))
    df = df.withColumn("publish", regexp_extract(col("publish"), r"(\w+ \d{1,2}, \d{4})", 1))
    df = df.withColumn("publish", to_date(col("publish"), "MMMM d, yyyy"))
    df = df.withColumn("publish", date_format(col("publish"), "dd/MM/yyyy"))
    df = df.drop("pages")
    return df
    
def convert():
    df = df.withColumn("pages_n", df["pages_n"].cast("int"))
    df = df.withColumn("prices", df["prices"].cast("float"))
    df = df.withColumn("onestar", df["onestar"].cast("int"))
    df = df.withColumn("twostars", df["twostars"].cast("int"))
    df = df.withColumn("threestars", df["threestars"].cast("int"))
    df = df.withColumn("fourstars", df["fourstars"].cast("int"))
    df = df.withColumn("fivestars", df["fivestars"].cast("int"))
    df = df.withColumn("publish", df["publish"].cast("date"))
    df = df.withColumn("rating", df["rating"].cast("float"))
    df = df.withColumn("ratingcount", df["ratingcount"].cast("int"))
    df = df.withColumn("reviews", df["reviews"].cast(""))

def save_data(df):
    df.write.format("mongo") \
        .mode("append") \
        .option("uri", "mongodb://localhost:27020") \
        .option("database", "db_goodread") \
        .option("collection", "tb_book_clean") \
        .save()
    

if __name__ == "__main__":
    spark = connect_mongo()
    df = load_data(spark)
    df = format_data(df)
    save_data(df)
    spark.stop()