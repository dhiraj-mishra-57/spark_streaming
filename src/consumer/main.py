from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

# Configuration settings for Kafka Producer
kafka_bootstrap_servers = "localhost:9092"  # Replace with your Kafka broker address

# Producing employee data to Kafka topic
kafka_topic = 'spark-streaming'

df = spark.readStream\
    .format('kafka')\
        .option('kafka.bootstrap.servers', kafka_bootstrap_servers)\
            .option('subscribe', kafka_topic)\
                .load()

# Kafka data is in binary format, so you need to cast it to string
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Define a query to output the data to the console
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

print('-' * 100)
print('HERE')
print('-' * 100)

# Await termination
query.awaitTermination()
