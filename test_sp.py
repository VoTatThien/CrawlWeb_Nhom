from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark Dataframe Introduction") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
        .getOrCreate()
df = spark.read.format("mongo").option("uri", "mongodb://mymongodb:27017") \
        .option("database", "db_goodread") \
        .option("collection", "tb_book") \
        .load()
df.show()
spark.stop()