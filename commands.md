Kafka
    Create Topic:
        *  kafka-topics.sh --create --topic spark-streaming --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
    Delete Topic:
        *  kafka-topics.sh --delete --topic spark-streaming --bootstrap-server localhost:9092
    Producer:
        * kafka-console-producer.sh --broker-list localhost:9092 --topic spark-streaming
    Consumer:
        * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic spark-streaming --from-beginning