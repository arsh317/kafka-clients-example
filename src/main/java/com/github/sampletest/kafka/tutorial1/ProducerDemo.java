package com.github.sampletest.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {

    public static void callback_func(RecordMetadata recordMetadata, Exception e, Logger logger){

        // executes every time a record is successfully sent or an exception is thrown
        if (e == null) {
            // the record was successfully sent
            logger.info("Received new metadata. \n" +
                    "Topic:" + recordMetadata.topic() + "\n" +
                    "Partition: " + recordMetadata.partition() + "\n" +
                    "Offset: " + recordMetadata.offset() + "\n" +
                    "Timestamp: " + recordMetadata.timestamp());
        } else {
            logger.error("Error while producing", e);
        }
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = "localhost:9092";

        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"1");

        // create the producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        for (int i=0; i<10; i++ ) {
            // create a producer record
            ProducerRecord<String, String> record1 =
                    new ProducerRecord<String, String>("emp-salary", "arsh","simple");

//            ProducerRecord<String, String> record2 =
//                    new ProducerRecord<String, String>("test3", "hello world " + Integer.toString(i));

            // send data - asynchronous
            producer.send(record1, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });

//            producer.send(record2, new Callback() {
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    // executes every time a record is successfully sent or an exception is thrown
//                    if (e == null) {
//                        // the record was successfully sent
//                        logger.info("Received new metadata. \n" +
//                                "Topic:" + recordMetadata.topic() + "\n" +
//                                "Partition: " + recordMetadata.partition() + "\n" +
//                                "Offset: " + recordMetadata.offset() + "\n" +
//                                "Timestamp: " + recordMetadata.timestamp());
//                    } else {
//                        logger.error("Error while producing", e);
//                    }
//                }
//            });
        }

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
