package com.santander;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);

        try (KafkaProducer<String, String> prod = new KafkaProducer<>(props)) {
            String topic = "topic-test";
            String key = "testKey";
            String value = "testValue";

            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
            prod.send(producerRecord, (metadata, e) -> {
                if (e != null) {
                    System.out.println("Send failed for record");
                } else {
                    System.out.println("Message sent successfully to topic " + metadata.topic());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
