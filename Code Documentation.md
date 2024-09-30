# SDE Mini Project Code Documentation

## Workflow

### config.py

#### Overview

The `config.py` file is responsible for loading and managing environment variables for the Spark City project. It uses the python-dotenv library to load variables from a `.env` file and makes them accessible through a configuration dictionary.

#### Dependencies

- os
- python-dotenv

#### Functionality

1. **Environment Variable Loading:**
   - The script uses `load_dotenv()` to load environment variables from a `.env` file located in the same directory.
   - This allows for easy management of sensitive information and configuration settings without hardcoding them in the source code.

2. **Configuration Dictionary:**
   - A configuration dictionary is created to store all the loaded environment variables.
   - This dictionary serves as a central point of access for configuration settings throughout the project.

#### Environment Variables

The following environment variables are expected to be defined in the `.env` file:

- AWS-related variables:
  - `AWS_ACCESS_KEY`: Access key for AWS services
  - `AWS_SECRET_KEY`: Secret key for AWS services
  - `AWS_BUCKET`: Name of the AWS S3 bucket

- MinIO-related variables:
  - `MINIO_ENDPOINT`: URL of the MinIO server
  - `MINIO_ACCESS_KEY`: Access key for MinIO
  - `MINIO_SECRET_KEY`: Secret key for MinIO
  - `MINIO_BUCKET`: Name of the MinIO bucket

#### Usage

To use this configuration in other parts of the project:

1. Import the configuration dictionary from this module:
   ```python
   from config import config
   ```

2. Access the configuration values as needed:
   ```python
   aws_access_key = config['AWS_ACCESS_KEY']
   ```

### main.py

#### Overview

This script simulates vehicle data, generates different data types (vehicle, GPS, traffic, weather, emergency incidents), and produces them to Kafka topics using Confluent Kafka. The data simulation mimics a journey from IIT Jodhpur to Delhi. It can be used for testing and real-time data streaming scenarios.

#### Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS`: Specifies the Kafka broker's address (default: "localhost:9092")
- `VEHICLE_TOPIC`: Topic name for vehicle data (default: "vehicle_data")
- `GPS_TOPIC`: Topic name for GPS data (default: "gps_data")
- `TRAFFIC_TOPIC`: Topic name for traffic camera data (default: "traffic_data")
- `WEATHER_TOPIC`: Topic name for weather data (default: "weather_data")
- `EMERGENCY_TOPIC`: Topic name for emergency incident data (default: "emergency_data")

#### Functions

1. `get_next_time()`
   - Updates and returns the global `startTime` by adding a random number of seconds (between 30 and 60) to simulate the next timestamp.

2. `generate_vehicle_data(deviceID)`
   - Parameters:
     - `deviceID` (str): The ID of the vehicle device
   - Returns: A dictionary containing simulated vehicle data, including location, speed, direction, and vehicle details.

3. `generate_gps_data(deviceID, timestamp, vehicleType='private')`
   - Parameters:
     - `deviceID` (str): The ID of the GPS device
     - `timestamp` (str): The timestamp of the data
     - `vehicleType` (str): The type of vehicle (default: 'private')
   - Returns: A dictionary with GPS data, including speed and direction.

4. `generate_traffic_camera_data(deviceID, timestamp, location, cameraID)`
   - Parameters:
     - `deviceID` (str): The ID of the traffic camera device
     - `timestamp` (str): The timestamp of the data
     - `location` (tuple): The coordinates (latitude, longitude)
     - `cameraID` (str): The ID of the camera
   - Returns: A dictionary representing traffic camera data.

5. `generate_weather_data(deviceID, timestamp, location)`
   - Parameters:
     - `deviceID` (str): The ID of the weather monitoring device
     - `timestamp` (str): The timestamp of the data
     - `location` (tuple): The coordinates
   - Returns: A dictionary with weather information, including temperature, weather conditions, precipitation, wind speed, humidity, and air quality index.

6. `generate_emergency_incident_data(deviceID, timestamp, location)`
   - Parameters:
     - `deviceID` (str): The ID of the emergency incident device
     - `timestamp` (str): The timestamp of the data
     - `location` (tuple): The coordinates
   - Returns: A dictionary representing emergency incident data, including type, status, and description.

7. `simulate_vehicle_movement()`
   - Simulates the movement of a vehicle by incrementing latitude and longitude values and adding random variations to mimic real vehicle movement.

8. `produce_data_to_kafka(producer, topic, data)`
   - Parameters:
     - `producer` (Confluent Kafka Producer): The Kafka producer object
     - `topic` (str): The topic to which data is sent
     - `data` (dict): The data to be serialized and produced to the Kafka topic

9. `simulate_journey(producer, deviceID)`
   - Parameters:
     - `producer` (Confluent Kafka Producer): The Kafka producer object
     - `deviceID` (str): The ID of the vehicle device
   - Simulates a vehicle's journey by generating various types of data (vehicle, GPS, traffic, weather, emergency incidents) and publishing them to Kafka topics until the vehicle reaches Delhi coordinates.

#### Main Execution

- Initializes a Kafka producer
- Calls `simulate_journey` to start the data simulation

### spark-city.py

#### Overview

This script sets up a Spark Streaming job to process real-time data from various smart city sensors. It reads data from multiple Kafka topics, processes the streams, and writes the results to S3 storage in Parquet format. The script is designed to handle vehicle, GPS, traffic, weather, and emergency incident data.

#### Dependencies

- PySpark
- Minio (S3-compatible object storage)
- Kafka

#### Configuration

The script relies on a separate `config.py` file (not provided) for configuration settings. It uses the following environment variables:

- `AWS_ACCESS_KEY`: AWS access key for S3 access
- `AWS_SECRET_KEY`: AWS secret key for S3 access

#### Main Components

1. **SparkSession Configuration**
   - Creates a SparkSession with necessary configurations for Kafka and S3 integration
   - Sets log level to "WARN" to minimize console output on executors

2. **Data Schemas**
   - Defines StructType schemas for different data types:
     - Vehicle Schema
     - GPS Schema
     - Traffic Schema
     - Weather Schema
     - Emergency Schema

#### Functions

1. `read_kafka_topic(topic, schema)`
   - Parameters:
     - `topic` (str): Kafka topic name
     - `schema` (StructType): Schema for the data
   - Returns: DataFrame
   - Reads data from a Kafka topic and applies the specified schema

2. `streamWriter(input, checkpointFolder, output)`
   - Parameters:
     - `input` (DataFrame): Input DataFrame to write
     - `checkpointFolder` (str): S3 path for checkpointing
     - `output` (str): S3 path for output data
   - Returns: StreamingQuery
   - Writes the input DataFrame to S3 in Parquet format

#### Main Execution Flow

1. Creates DataFrames for each data type by reading from corresponding Kafka topics
2. Initiates streaming queries to write each DataFrame to S3
3. Awaits termination of the last query

#### Usage

Run the script to start the Spark Streaming job. It will continuously process data from Kafka topics and store the results in S3.

### minio-spark-city.py

#### Overview

This script sets up a Spark Streaming job to process real-time data from various smart city sensors. It reads data from multiple Kafka topics, processes the streams, and writes the results to MinIO storage (S3-compatible) in Parquet format. The script is designed to handle vehicle, GPS, traffic, weather, and emergency incident data.

#### Dependencies

- PySpark
- Minio (S3-compatible object storage)
- Kafka

#### Configuration

The script relies on a separate `config.py` file for configuration settings. It uses the following environment variables:

- `MINIO_ENDPOINT`: URL of the MinIO server
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key
- `MINIO_BUCKET`: Name of the MinIO bucket to use for storage

#### Main Components

1. **MinIO Client Configuration**
   - Initializes a MinIO client using the provided configuration
   - Prints the MinIO configuration for debugging purposes

2. **SparkSession Configuration**
   - Creates a SparkSession with necessary configurations for Kafka and MinIO (S3) integration
   - Sets log level to "WARN" to minimize console output on executors

3. **Data Schemas**
   - Defines StructType schemas for different data types:
     - Vehicle Schema
     - GPS Schema
     - Traffic Schema
     - Weather Schema
     - Emergency Schema

#### Functions

1. `read_kafka_topic(topic, schema)`
   - Parameters:
     - `topic` (str): Kafka topic name
     - `schema` (StructType): Schema for the data
   - Returns: DataFrame
   - Reads data from a Kafka topic and applies the specified schema

2. `streamWriter(input_df, checkpoint_folder, output_path)`
   - Parameters:
     - `input_df` (DataFrame): Input DataFrame to write
     - `checkpoint_folder` (str): S3 path for checkpointing
     - `output_path` (str): S3 path for output data
   - Returns: StreamingQuery
   - Writes the input DataFrame to MinIO in Parquet format

#### Main Execution Flow

1. Initializes MinIO client and SparkSession
2. Defines schemas for each data type
3. Creates DataFrames for each data type by reading from corresponding Kafka topics
4. Initiates streaming queries to write each DataFrame to MinIO
5. Awaits termination of the last query

#### Usage

Run the script to start the Spark Streaming job. It will continuously process data from Kafka topics and store the results in MinIO.
