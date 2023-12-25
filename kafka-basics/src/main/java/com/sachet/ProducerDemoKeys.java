package com.sachet;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoKeys.class);

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
        for (int i=0; i<2; i++) {

            for (int j=0; j<10; j++) {

                int outer_loop = i;
                String topic = "demo_java";
                String key = "id_"+j;
                String data = "hello kafka callback "+ i;

                    //create a producer-record
                    ProducerRecord<String, String> producerRecord = new
                            ProducerRecord<>(topic, key, data);


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
                            LOGGER.info("For "+outer_loop+"Key: "+key+" | Partition: "+recordMetadata.partition()+"\n");
                        }else {
                            LOGGER.error("Error was thrown while sending\n"+e.getMessage());
                        }
                    });

                    Thread.sleep(500);

                    //flush and close the producer
//            kafkaProducer.flush(); --> This step taken care when producer closed is taken care on try with resources
                }

            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
// Partition -> index_id
// 0         -> 0_1, 0_3, 0_6, 1_1, 1_3, 1_6
// 1         -> 0_0, 0_8, 1_0, 1_8
// 2         -> 0_2, 0_4, 0_5, 0_7, 0_9, 1_2, 1_4, 1_5, 1_7, 1_9