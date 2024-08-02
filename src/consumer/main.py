from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark-streaming").getOrCreate()
spark.stop()
