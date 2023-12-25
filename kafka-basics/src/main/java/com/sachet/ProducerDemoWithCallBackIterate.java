package com.sachet;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBackIterate {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallBackIterate.class);

    public static void main(String[] args) {
        LOGGER.info("Producer demo");

        // Steps to create kafka producer --->
        //create producer properties, basically properties of producer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        //create the producer
        try(KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {

            for (int j=0; j<10; j++) {
                for(int i=0; i< 30; i++) {
                    //create a producer-record
                    ProducerRecord<String, String> producerRecord = new
                            ProducerRecord<>("demo_java", "hello kafka callback3");


                    //send data - asynchronous operation
//            kafkaProducer.send(producerRecord, new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//
//                }
//            });
                    kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                        //executes everytime a record is sent or an exception is thrown

                        if (e == null) {
                            LOGGER.info("New Metadata Received\n"
                                    +"Topic: "+recordMetadata.topic()+"\n"
                                    +"Partition: "+recordMetadata.partition()+"\n"
                                    +"Offset: "+recordMetadata.offset()+"\n"
                                    +"TimeStamp: "+recordMetadata.timestamp()+"\n"
                            );
                        }else {
                            LOGGER.error("Error was thrown while sending\n"+e.getMessage());
                        }
                    });
                }
                Thread.sleep(1000);
            }

            //flush and close the producer
//            kafkaProducer.flush(); --> This step taken care when producer closed is taken care on try with resources
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
