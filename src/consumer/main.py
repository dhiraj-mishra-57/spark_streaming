from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StructType, StringType, IntegerType


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
kafka_topic = 'sparkStreaming'

df = spark.readStream\
    .format('kafka')\
        .option('kafka.bootstrap.servers', kafka_bootstrap_servers)\
            .option('subscribe', kafka_topic)\
            .option('startingoffsets', 'earliest')\
                .load()

# Kafka data is in binary format, so you need to cast it to string
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

json_schema = (
    StructType(
        [StructField("employee_id", IntegerType(), True),
         StructField("first_name", StringType(), True),
         StructField("last_name", StringType(), True),
         StructField("department_id", IntegerType(), True),
         StructField("job_title", StringType(), True),
         StructField("salary", StringType(), True),
         StructField("hire_date", StringType(), True),
         StructField("location", StringType(), True),
         StructField("age", IntegerType(), True),
         StructField("performance_score", IntegerType(), True)]
         )
         )

newdf = df.withColumn("value_json", from_json(col("value"), json_schema))

print(newdf.printSchema())

final_df = newdf.selectExpr("value_json.*")
fdf = final_df.drop(col("performance_score"))

checkpointing_dir = r"V:\Upskill\Github\Repositories\checkpointing"

# Define a query to output the data to the console
# Await termination
df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", checkpointing_dir) \
    .start()\
    .awaitTermination()
