from pyspark.sql import SparkSession
import mysql.connector
from mysql.connector import errorcode
from pyspark.sql.types import StructType,StructField,StringType,TimestampType,DoubleType,IntegerType
from config import configuration

def main():
    # MySQL connection details
    mysql_url = "jdbc:mysql://host.docker.internal:3306/streaming_data"
    mysql_user = "root"
    mysql_password = "Iit29122002*"
    
    minio_access_key = configuration.get("MINIO_ACCESS_KEY")
    minio_secret_key = configuration.get("MINIO_SECRET_KEY")
    minio_endpoint = configuration.get("MINIO_ENDPOINT")   
    minio_bucket = configuration.get("MINIO_BUCKET")
    
    # print(f"minio_access_key: {minio_access_key}, minio_secret_key: {minio_secret_key}, minio_endpoint: {minio_endpoint}, minio_bucket: {minio_bucket}")
    # if not minio_access_key or not minio_secret_key or minio_endpoint or minio_bucket:
    #     raise EnvironmentError("MINIO_ACCESS_KEY_ID and MINIO_SECRET_ACCESS_KEY must be set as environment variables.")
    
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("GlueCrawlerReplication") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
        
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
    
    # List of datasets with their paths and table names
    datasets = [
        ("vehicle_data", f"s3a://{minio_bucket}/data/vehicle_data/", vehicleSchema),
        ("gps_data", f"s3a://{minio_bucket}/data/gps_data/", gpsSchema),
        ("traffic_data", f"s3a://{minio_bucket}/data/traffic_data/", trafficSchema),
        ("weather_data", f"s3a://{minio_bucket}/data/weather_data/", weatherSchema),
        ("emergency_data", f"s3a://{minio_bucket}/data/emergency_data/", emergencySchema)
    ]
    
    # Process each dataset
    for table_name, path, schema in datasets:
        # Read data from MinIO as a streaming DataFrame
        df = spark.readStream.format("parquet").schema(schema).load(path)
        df.printSchema()
        
        # Create table in MySQL based on inferred schema
        create_table_in_mysql(df, table_name, mysql_user, mysql_password)
        
        # Stream data to MySQL
        stream_to_mysql(df, table_name, mysql_url, mysql_user, mysql_password)

    spark.streams.awaitAnyTermination()

def create_table_in_mysql(df, table_name, mysql_user, mysql_password):
    # Infer the schema from the DataFrame
    print("creating tablee *********************************************")
    schema = df.schema
    
    # Convert PySpark schema to MySQL schema
    mysql_schema = []
    for field in schema.fields:
        mysql_type = spark_to_mysql_type(field.dataType)
        mysql_schema.append(f"`{field.name}` {mysql_type}")
    
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(mysql_schema)});"
    
    connection = None
    cursor = None    
    
    # Connect to MySQL and create the table
    try:
        connection = mysql.connector.connect(
            host="host.docker.internal",
            port=3306,
            user=mysql_user,
            password=mysql_password,
            database="streaming_data"
        )
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        elif err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        else:
            print('#############################')
            print(err)

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def spark_to_mysql_type(spark_type):
    # Convert PySpark data types to MySQL data types
    if spark_type.simpleString() == 'string':
        return "VARCHAR(255)"
    elif spark_type.simpleString() == 'integer':
        return "INT"
    elif spark_type.simpleString() == 'double':
        return "DOUBLE"
    elif spark_type.simpleString() == 'timestamp':
        return "DATETIME"
    else:
        return "VARCHAR(255)"  # Default to VARCHAR for unknown types

# def spark_to_mysql_type(spark_type):
#     # Convert PySpark data types to MySQL data types
#     if isinstance(spark_type, StringType):
#         return "VARCHAR(255)"
#     elif isinstance(spark_type, IntegerType):
#         return "INT"
#     elif isinstance(spark_type, DoubleType):
#         return "DOUBLE"
#     elif isinstance(spark_type, TimestampType):
#         return "DATETIME"
#     else:
#         return "VARCHAR(255)"

def stream_to_mysql(input_df, table_name, mysql_url, mysql_user, mysql_password):
    def write_to_mysql(batch_df, batch_id):
        print('********************************************************')
        print(f"Writing batch {batch_id} for table {table_name} to MySQL")
        batch_df.show()
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", mysql_url) \
                .option("dbtable", table_name) \
                .option("user", mysql_user) \
                .option("password", mysql_password) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("append") \
                .save()
            print(f"Batch {batch_id} written to table {table_name}")
        except Exception as e:
            print(f"Error writing batch {batch_id} to MySQL: {e}")
    
    query = input_df.writeStream \
        .foreachBatch(write_to_mysql) \
        .start()

if __name__ == '__main__':
    main()
