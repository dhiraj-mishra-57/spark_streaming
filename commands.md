Kafka
    List Topic:
        *   kafka-topics.sh --list --bootstrap-server localhost:9092
    Create Topic:
        *  kafka-topics.sh --create --topic sparkStreaming --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
    Delete Topic:
        *  kafka-topics.sh --delete --topic sparkStreaming --bootstrap-server localhost:9092
    Producer:
        * kafka-console-producer.sh --broker-list localhost:9092 --topic sparkStreaming
    Consumer:
        * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sparkStreaming --from-beginning

SparkStreaming
    Submit job
        * spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 --jars "V:\Upskill\Github\Repositories\real_time_streaming\documentation\postgres_driver\postgresql-42.7.4.jar" main.py