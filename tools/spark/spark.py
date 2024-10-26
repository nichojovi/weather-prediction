from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def write_to_mongodb(batch_df, batch_id):
    batch_df.show(truncate=False)
    batch_df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", "mongodb://localhost:27017/weather_db.weather_data") \
        .save()

kafka_topic_name = "weather_data"
kafka_server = "localhost:9092"

spark = SparkSession.builder.appName("WeatherDataProcessing") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/weather_db.weather_data") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,"
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/weather_db.weather_data") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/weather_db.weather_data") \
    .getOrCreate()

schema = StructType([
    StructField("formatted_date", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("precip_type", StringType(), True),
    StructField("temperature", StringType(), True),
    StructField("apparent_temperature", StringType(), True),
    StructField("humidity", StringType(), True),
    StructField("wind_speed", StringType(), True),
    StructField("wind_bearing", StringType(), True),
    StructField("visibility", StringType(), True),
    StructField("cloud_cover", StringType(), True),
    StructField("pressure", StringType(), True),
    StructField("daily_summary", StringType(), True)
])

weather_data_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", kafka_topic_name) \
    .load() \
    .selectExpr("CAST(value AS STRING)")

weather_data_df = weather_data_df \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query = weather_data_df.writeStream \
    .foreachBatch(write_to_mongodb) \
    .start()

query.awaitTermination()