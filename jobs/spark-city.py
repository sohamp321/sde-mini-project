from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming").getOrCreate()
    pass

if __name__ == "__main__":
    main()