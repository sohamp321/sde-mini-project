# IoT Data Simulation and Analysis Pipeline

## Project Overview

This project simulates IoT data streams for smart city applications, including vehicles, GPS, traffic, weather, and emergency incidents. The generated data is streamed to Apache Kafka, processed using Apache Spark, and stored in AWS S3 for further analysis. The pipeline leverages AWS Glue, Athena, and Redshift for data discovery, querying, and warehousing.

## Prerequisites

- **Python Environment**: Set up a virtual environment for running Python scripts.
- **Docker**: Used to containerize and run necessary services such as Kafka and Spark.
- **Apache Kafka**: For real-time data streaming.
- **Apache Spark**: For data processing and transformation.
- **AWS Services**: S3, Glue, Athena, and Redshift for storage and querying.

## Project Structure

```bash
.
├── jobs/
│   ├── config.py                     # Configuration settings
│   ├── main.py                       # Entry point for generating and streaming IoT data
│   ├── mini-spark-city.py            # Mini Spark job for processing a subset of the streamed data
│   ├── minio-to-mysql.py             # Script for transferring data from MinIO to MySQL
│   ├── mysql-connector-java-8.0.25.jar # MySQL connector JAR for Java applications
│   └── spark-city.py                 # Main Spark job for processing the streamed data
```

## Setup Instructions

### 1. Activate Python Environment

Activate your Python virtual environment to manage dependencies:

```bash
smartCityEnv\Scripts\activate
```
### 2. Install Python Dependencies

Install the required Python packages by running the following command in the terminal:

```bash
pip install -r requirements.txt
```

### 3. Docker Setup

Start the necessary services using Docker by running the following command in the terminal:

```bash
docker-compose up --build
```

### 4. Running Spark Job

Once the Docker containers are up and running, you can submit the Apache Spark job that processes data streams from Kafka by executing the following command:

1. For Cloud
```bash
docker exec -it sde-mini-project-spark-master-1 spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 \
  jobs/spark-city.py
```

2. For non-cloud
```bash
docker exec --env-file .env -it sde-mini-project-spark-master-1 spark-submit \
  --master spark://spark-master:7077  \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469,mysql:mysql-connector-java:8.0.25  \
  jobs/minio-to-mysql.py
```
### 5. AWS Configuration

Ensure that the following AWS services are properly configured:

- **AWS S3**: Set up an S3 bucket with the necessary policies and permissions to securely store the processed data from Spark.
- **AWS IAM Roles**: Manage appropriate IAM roles and policies that grant access to your AWS services.
- **AWS Glue Crawlers**: Set up AWS Glue Crawlers to organize and catalog the data stored in S3 for querying.
- **AWS Athena**: Use Athena to query the data stored in S3 for analysis.
- **AWS Redshift**: Load the processed data into Redshift from Glue for advanced analysis and warehousing.

### 6. IoT Data Generation

The IoT data simulation is handled by the `main.py` script in the `jobs/` directory. This script generates data for the following:

- **Vehicle data**: Simulates vehicle telemetry like speed, fuel level, etc.
- **GPS data**: Simulates GPS location tracking.
- **Traffic data**: Simulates smart traffic management data.
- **Weather data**: Simulates real-time weather conditions.
- **Emergency data**: Simulates emergency incidents like accidents or hazards.

The generated data streams are sent to Kafka for real-time processing by Apache Spark.

### 7. Data Streaming and Processing

The generated IoT data is streamed into **Apache Kafka**, which manages the flow of the data for real-time consumption. **Apache Spark** is used to:

- **Consume Data**: Spark consumes the IoT data streams from Kafka.
- **Apply Schema**: The schema for the data is defined in the `spark-city.py` script.
- **Store in S3**: The processed data is then written to AWS S3 for long-term storage and analysis.

### 8. Additional Information

#### AWS Glue and Redshift Setup

After the data is processed and stored in AWS S3, you can use AWS Glue Crawlers to discover the schema and organize the data for querying in AWS Athena. You can also load this data into AWS Redshift for more complex queries and analysis. Make sure that the IAM roles have the necessary permissions to interact with these services.

#### Querying and Visualization

- **AWS Athena**: You can use Athena to run SQL queries on the data stored in S3. This is useful for quick, serverless querying of large datasets.
- **AWS Redshift**: For more complex analysis, you can load the data into AWS Redshift and run advanced queries using an SQL client such as DBeaver.

### Conclusion

This project sets up an end-to-end pipeline for generating, processing, and analyzing IoT data streams for smart city infrastructure. By leveraging Docker for containerization, Apache Kafka for streaming, Apache Spark for processing, and AWS for storage and querying, this pipeline provides a scalable, real-time data processing and analysis solution. Future improvements can enhance the system's capabilities and performance for large-scale smart city simulations.

### Contributors

| Name                              | Roll Number |
|----------------------------------- |-------------|
| [Arun Raghav S](https://github.com/arun-raghav-s)    | B21CS015          |
| [Kashvi Jain](https://github.com/kashvi0)  | B21CS037          |
| [Soham Parikh](https://github.com/sohamp321)    | B21CS074          |
