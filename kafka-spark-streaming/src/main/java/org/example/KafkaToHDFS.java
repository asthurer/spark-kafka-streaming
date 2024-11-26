package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class KafkaToHDFS {
    public static void main(String[] args) {

        // Assume start and end timestamps are provided as arguments or environment variables
        String startTime = "2024-11-25T00:00:00"; // Example: ISO-8601 format
        String endTime = "2024-11-25T23:59:59";

        // Load configuration from application.conf
        Config config = ConfigFactory.load("application.conf");

        // Read configuration values
        String kafkaBootstrapServers = config.getString("kafka.bootstrapServers");
        String kafkaTopic = config.getString("kafka.topic");
        String kafkaSecurityProtocol = config.getString("kafka.securityProtocol");
        String kafkaSaslMechanism = config.getString("kafka.saslMechanism");
        String kafkaUsername = config.getString("kafka.username");
        String kafkaPassword = config.getString("kafka.password");
        String hdfsOutputPath = config.getString("hdfs.outputPath");
        String keytabPath = config.getString("security.keytabPath");
        String principal = config.getString("security.principal");

        // Set up Spark configuration
        SparkConf sparkConf = new SparkConf().setAppName("KafkaToHDFSApp");
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        // Kerberos authentication (if applicable)
        if (!keytabPath.isEmpty() && !principal.isEmpty()) {
            System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
            spark.sparkContext().conf().set("spark.yarn.principal", principal);
            spark.sparkContext().conf().set("spark.yarn.keytab", keytabPath);
        }

        // Read data from Kafka
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", kafkaTopic)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .option("kafka.security.protocol", kafkaSecurityProtocol)
                .option("kafka.sasl.mechanism", kafkaSaslMechanism)
                .option("kafka.sasl.jaas.config", String.format(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                        kafkaUsername, kafkaPassword))
                .load();

        // Process data: Assuming Kafka value is JSON
        Dataset<Row> processedData = kafkaStream.selectExpr("CAST(value AS STRING) as message");

        // Extract Kafka message value and timestamp
        Dataset<Row> filteredData = kafkaStream
                .selectExpr("CAST(value AS STRING) as message", "timestamp")
                .filter(col("timestamp").geq(lit(startTime))
                        .and(col("timestamp").leq(lit(endTime))));

        // Write data to HDFS
        try {
            StreamingQuery query = processedData.writeStream()
                    .format("parquet")
                    .outputMode("append")
                    .option("checkpointLocation", hdfsOutputPath + "/checkpoint")
                    .option("path", hdfsOutputPath)
                    .trigger(Trigger.ProcessingTime("1 minute"))
                    .start();

            // Await termination and handle exceptions
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            System.err.println("Streaming query timed out: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
