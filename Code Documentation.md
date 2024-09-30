# SDE Mini Project Code Documentation

## File Documentation and Code Walkthrough

### 1. config.py

#### Purpose
This file manages the loading of environment variables, specifically AWS credentials, for use in the project.

#### Code Walkthrough
1. The script imports necessary modules: `os` and `load_dotenv` from `dotenv`.
2. It calls `load_dotenv()` to load environment variables from a `.env` file.
3. A `configuration` dictionary is created, storing AWS access and secret keys retrieved from environment variables.

#### Usage
This file is imported in other scripts to access the AWS credentials securely. The use of environment variables enhances security by keeping sensitive information out of the codebase.

### 2. main.py

#### Purpose
This script simulates a vehicle journey, generating various types of data (vehicle, GPS, traffic, weather, and emergency) and sending them to Kafka topics.

#### Code Walkthrough
1. Constants and Configuration:
   - Defines coordinates for IIT Jodhpur and Delhi.
   - Calculates latitude and longitude increments for simulating movement.
   - Sets up Kafka topics and bootstrap servers using environment variables.

2. Data Generation Functions:
   - `generate_vehicle_data`: Creates vehicle data with unique ID, location, speed, and vehicle details.
   - `generate_gps_data`: Produces GPS data including speed and direction.
   - `generate_traffic_camera_data`: Simulates traffic camera snapshots.
   - `generate_weather_data`: Creates weather data with temperature, conditions, and air quality.
   - `generate_emergency_incident_data`: Simulates emergency incidents.

3. Kafka Production:
   - `delivery_report`: Callback function for Kafka message delivery status.
   - `json_serializer`: Custom JSON serializer for handling UUID objects.
   - `produce_data_to_kafka`: Sends generated data to specified Kafka topics.

4. Simulation Functions:
   - `simulate_vehicle_movement`: Updates vehicle location to simulate movement.
   - `simulate_journey`: Main simulation loop generating all data types and sending to Kafka.

5. Main Execution:
   - Sets up Kafka producer configuration.
   - Initiates the journey simulation with a specified device ID.

#### Usage
Run this script to start the simulation of a vehicle journey. It continuously generates data and sends it to Kafka topics until the simulated journey is complete or interrupted.

### 3. spark-city.py

#### Purpose
This script sets up a Spark streaming job to process data from Kafka topics and write it to S3.

#### Code Walkthrough
1. Spark Session Configuration:
   - Builds a SparkSession with necessary configurations for Kafka and S3.
   - Sets up AWS credentials for S3 access.

2. Schema Definitions:
   - Defines schemas for vehicle, GPS, traffic, weather, and emergency data using `StructType` and `StructField`.

3. Kafka Reading Function:
   - `read_kafka_topic`: Reads data from a Kafka topic, applies the specified schema, and sets up watermarking.

4. Stream Writing Function:
   - `streamWriter`: Configures the output stream to write data in Parquet format to S3.

5. Main Execution:
   - Creates DataFrames for each data type by reading from Kafka topics.
   - Sets up streaming queries to write each DataFrame to S3.
   - Waits for query termination.

#### Usage
This script should be run in a Spark environment. It continuously reads data from Kafka topics, processes it, and writes it to S3 in Parquet format.

### 4. docker-compose.yml

#### Purpose
This file defines and configures the services required for the project, including Zookeeper, Kafka, and the Spark application.

#### Code Walkthrough
1. Zookeeper Service:
   - Uses `confluentinc/cp-zookeeper` image.
   - Configures essential Zookeeper properties for Kafka cluster management.

2. Kafka Broker Service:
   - Uses `confluentinc/cp-kafka` image.
   - Sets up a Kafka broker with specified listener configurations and topic creation settings.

3. Spark Service:
   - Uses a custom Spark image built from the current directory.
   - Mounts the local `jobs` directory to `/opt/spark-apps` in the container.
   - Configures environment variables for Spark master and application locations.

#### Usage
Use `docker-compose up` to start all services defined in this file. This sets up the entire environment needed for the data streaming pipeline.

## System Workflow

1. Data Generation (main.py):
   - Simulation starts, generating various types of data.
   - Data is continuously produced and sent to respective Kafka topics.

2. Message Queuing (Kafka):
   - Kafka receives the generated data on different topics.
   - Acts as a buffer, storing messages and allowing for decoupled, scalable processing.

3. Data Processing (spark-city.py):
   - Spark Streaming job consumes messages from Kafka topics.
   - Applies defined schemas to incoming data, structuring it for analysis.
   - Processes data in micro-batches for near real-time analysis.

4. Data Storage (S3):
   - Processed data is written to S3 in Parquet format.
   - Data is organized in the S3 bucket based on data type.
   - Parquet format allows for efficient storage and quick query performance.

5. Continuous Operation:
   - System runs continuously, with data flowing from simulation through Kafka to Spark and finally to S3.
   - Spark's checkpointing mechanism ensures fault tolerance and exactly-once processing semantics.