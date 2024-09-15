from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StructType, StringType, IntegerType


# Initialize Spark session with the configuration
spark = SparkSession.builder \
    .appName("KafkaExample") \
    .config("spark.streaming.stopGracefullyonShutdown", True)\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")\
    .config("spark.jars", r"V:\Upskill\Github\Repositories\real_time_streaming\documentation\postgres_driver\postgresql-42.7.4.jar")\
    .config("spark.sql.shuffle.partitions", 4)\
    .master("local[*]")\
    .getOrCreate()


# Configuration settings for spark checkpointing
checkpointing_dir = r"V:\Upskill\Github\Repositories\checkpointing"

# Configuration settings for Kafka Producer
kafka_bootstrap_servers = "localhost:9092"

# Producing employee data to Kafka topic
kafka_topic = 'sparkStreaming'

df = spark.readStream\
    .format('kafka')\
        .option('kafka.bootstrap.servers', kafka_bootstrap_servers)\
            .option('subscribe', kafka_topic)\
            .option('startingoffsets', 'latest')\
                .load()

# Kafka data is in binary format, so you need to cast it to string
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

json_schema = (
    StructType(
        [StructField("employee_id", IntegerType(), True),
         StructField("first_name", StringType(), True),
         StructField("last_name", StringType(), True),
         StructField("department_name", StringType(), True),
         StructField("job_title", StringType(), True),
         StructField("salary", StringType(), True),
         StructField("hire_date", StringType(), True),
         StructField("location", StringType(), True),
         StructField("age", IntegerType(), True),
         StructField("performance_score", IntegerType(), True)]))

newdf = df.withColumn("value_json", from_json(col("value"), json_schema))

final_df = newdf.selectExpr("value_json.*")

# # Define a query to output the data to the console
# final_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("checkpointLocation", checkpointing_dir) \
#     .start()\
#     .awaitTermination()


# Define JDBC connection properties
port = 5432
host = "localhost"
table = "employees"
password = "password"
username = "postgres"
database = "streaming"

jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
connection_properties = {
    "user": username,
    "password": password,
    "driver": "org.postgresql.Driver"
}

def process_batches(df, epoch_id):
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("url", jdbc_url) \
        .option("dbtable", "employees") \
        .option("user", username) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver")\
        .save()
    print("~~~~~~~~~~~~~~~~~~~~~~ data loaded ~~~~~~~~~~~~~~~~~~~~~~")
        

# Define a query to postgre table: employees
query = final_df.writeStream \
                    .foreachBatch(process_batches) \
                        .outputMode("append") \
                            .start()\
                                .awaitTermination()