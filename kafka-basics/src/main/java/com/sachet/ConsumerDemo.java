package com.sachet;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        LOGGER.info("Producer demo");
        String groupId = "my-java-groupId";
        String topic = "demo_java";

        // Steps to create kafka producer --->
        //create consumer properties, basically properties of consumer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); //reading data from beginning

        //create the consumer
        try(KafkaConsumer<String , String> consumer = new KafkaConsumer<>(properties)) {
            //subscribe to a topic
            consumer.subscribe(List.of(topic));

            //poll the data
            while(true) {
                LOGGER.info("Polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record: records) {
                    LOGGER.info("Key: "+record.key()+", "+"Value: "+record.value());
                    LOGGER.info("Partition: "+record.partition()+", "+"Offset: "+record.offset());
                }
            }
        }
    }

}
// Partition -> index_id
// 0         -> 0_1, 0_3, 0_6, 1_1, 1_3, 1_6
// 1         -> 0_0, 0_8, 1_0, 1_8
// 2         -> 0_2, 0_4, 0_5, 0_7, 0_9, 1_2, 1_4, 1_5, 1_7, 1_9