package com.sachet;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        LOGGER.info("Producer demo");

        // Steps to create kafka producer --->
        //create producer properties, basically properties of producer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the producer
        try(KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            //create a producer-record
            ProducerRecord<String, String> producerRecord = new
                    ProducerRecord<>("demo_java", "hello kafka");


            //send data - asynchronous operation
            kafkaProducer.send(producerRecord);

            //flush and close the producer
//            kafkaProducer.flush(); --> This step taken care when producer closed is taken care on try with resources
        }
    }

}
