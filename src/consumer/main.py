from pyspark import SparkConf
from pyspark.sql import SparkSession

# Disable Hadoop native libraries
conf = SparkConf()
conf.set("spark.hadoop.io.nativeio.enable", "false")

# Initialize Spark session with the configuration
spark = SparkSession.builder \
    .appName("KafkaExample") \
    .config("spark.streaming.stopGracefullyonShutdown", True)\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")\
    .config("spark.sql.shuffle.partitions", 4)\
    .master("local[*]")\
    .getOrCreate()

# Configuration settings for Kafka Producer
kafka_bootstrap_servers = "localhost:9092"  # Replace with your Kafka broker address

# Producing employee data to Kafka topic
kafka_topic = 'spark-streaming'

df = spark.readStream\
    .format('kafka')\
        .option('kafka.bootstrap.servers', kafka_bootstrap_servers)\
            .option('subscribe', kafka_topic)\
            .option('startingoffsets', 'earliest')\
                .load()

# Kafka data is in binary format, so you need to cast it to string
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Define a query to output the data to the console
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "C:\\Users\\dhira") \
    .start()

# Await termination
query.awaitTermination()
