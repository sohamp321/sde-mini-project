from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col
from pyspark.sql.dataframe import DataFrame
from minio import Minio
from minio.error import S3Error
import os

def main():
    # Load MinIO configuration from environment variables
    minio_url = configuration.get('MINIO_ENDPOINT')
    access_key = configuration.get('MINIO_ACCESS_KEY')
    secret_key = configuration.get('MINIO_SECRET_KEY')
    bucket_name = configuration.get('MINIO_BUCKET')
    
    print(f"MinIO URL: {minio_url}, Access Key: {access_key}, Secret Key: {secret_key}, Bucket Name: {bucket_name}")
    
    # Initialize MinIO client
    minio_client = Minio(
        minio_url.replace("http://", ""),  # No need to strip 'http://' if using full URL
        access_key=access_key,
        secret_key=secret_key,
        secure=False  # Set to True if using HTTPS
    )
    
    # Initialize Spark session with appropriate configurations
    spark = SparkSession.builder.appName("SmartCityStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_url) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Adjust log level to minimize output
    spark.sparkContext.setLogLevel("WARN")

    # Define schemas for each data stream
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceID", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])

    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceID", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceID", StringType(), True),
        StructField("cameraID", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceID", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True)
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceID", StringType(), True),
        StructField("incidentID", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])

    # Function to read data from Kafka topics
    def read_kafka_topic(topic, schema):
        return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", 'broker:29092')
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
            .withWatermark("timestamp", "2 minutes")
        )

    # Function to write stream to MinIO (via S3 API)
    def streamWriter(input_df: DataFrame, checkpoint_folder: str, output_path: str):
        return (input_df.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_folder)
            .start()
        )

    # Read from Kafka topics and apply schemas
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema)
    gpsDF = read_kafka_topic('gps_data', gpsSchema)
    trafficDF = read_kafka_topic('traffic_data', trafficSchema)
    weatherDF = read_kafka_topic('weather_data', weatherSchema)
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema)

    # Write the streams to MinIO (S3) storage
    query1 = streamWriter(vehicleDF, f"s3a://{bucket_name}/checkpoints/vehicle_data", f"s3a://{bucket_name}/data/vehicle_data")
    query2 = streamWriter(gpsDF, f"s3a://{bucket_name}/checkpoints/gps_data", f"s3a://{bucket_name}/data/gps_data")
    query3 = streamWriter(trafficDF, f"s3a://{bucket_name}/checkpoints/traffic_data", f"s3a://{bucket_name}/data/traffic_data")
    query4 = streamWriter(weatherDF, f"s3a://{bucket_name}/checkpoints/weather_data", f"s3a://{bucket_name}/data/weather_data")
    query5 = streamWriter(emergencyDF, f"s3a://{bucket_name}/checkpoints/emergency_data", f"s3a://{bucket_name}/data/emergency_data")

    # Await termination of streaming queries
    query5.awaitTermination()

if __name__ == '__main__':
    main()
