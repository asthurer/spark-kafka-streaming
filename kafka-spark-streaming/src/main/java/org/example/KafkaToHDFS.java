package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class KafkaToHDFS {
    public static void main(String[] args) throws InterruptedException {
        // Create Spark Configuration
        SparkConf conf = new SparkConf().setAppName("KafkaToHDFS");

        // Create Java Streaming Context
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        // Kafka Configuration
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "group1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test-topic");

        // Create Kafka Direct Stream
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        // Process the stream and save to HDFS
        stream.foreachRDD(rdd -> {
            rdd.map(record -> record.value()).saveAsTextFile("hdfs://localhost:9000/user/output/");
        });

        // Start the context
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
