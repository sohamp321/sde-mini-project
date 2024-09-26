from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql.types import StructType,StructField,StringType,TimestampType,DoubleType,IntegerType

def main():
    spark=SparkSession.builder.appName("SmartCityStreaming")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0","org.apache.hadoop:hadoop-aws:3.3.1","com.amazonaws:aws-java-sdk:1.11.469")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()
        
    #Adjust the lof level to minimize console output on executors
    spark.sparkContext.setLogLevel("WARN")
    
    #vehicle schema
    vehicleSchema=StructType([
        StructField("id",StringType(),True),
        StructField("deviceID",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("location",StringType(),True),
        StructField("speed",DoubleType(),True),
        StructField("direction",StringType(),True),
        StructField("make",StringType(),True),
        StructField("model",StringType(),True),
        StructField("year",IntegerType(),True),
        StructField("fuelType",StringType(),True)
        
    ])
    
    gpsSchema=StructType([
        StructField("id",StringType(),True),
        StructField("deviceID",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("speed",DoubleType(),True),
        StructField("direction",StringType(),True),
        StructField("vehicleType",StringType(),True)
    ])
    
    trafficSchema=StructType([
        StructField("id",StringType(),True),
        StructField("deviceID",StringType(),True),
        StructField("cameraID",StringType(),True),
        StructField("location",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("snapshot",StringType(),True)
    ])
    
    weatherSchema=StructType([
        StructField("id",StringType(),True),
        StructField("deviceID",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("location",StringType(),True),
        StructField("temperature",DoubleType(),True),
        StructField("weatherCondition",StringType(),True),
        StructField("precipitation",DoubleType(),True),
        StructField("windSpeed",DoubleType(),True),
        StructField("humidity",IntegerType(),True),
        StructField("airQualityIndex",DoubleType(),True)
    ])
     
    emergencySchema=StructType([
        StructField("id",StringType(),True),
        StructField("deviceID",StringType(),True),
        StructField("incidentID",StringType(),True),
        StructField("type",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("location",StringType(),True),
        StructField("status",StringType(),True),
        StructField("description",StringType(),True)
    ])

if __name__ == '__main__':
    main()