spark-submit \
--class KafkaToHDFS \
--master yarn \
--deploy-mode cluster \
--files /path/to/application.conf \
target/kafka-to-hdfs-1.0-SNAPSHOT.jar
