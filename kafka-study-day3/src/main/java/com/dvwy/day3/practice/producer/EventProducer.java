package com.dvwy.day3.practice.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class EventProducer {
    private final String TOPIC = "web-raw-log";
    private KafkaProducer<String, String> producer;

    public EventProducer(Properties props){
        producer = new KafkaProducer<>(props);
    }

    public void send(String key, String jsonValue) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, jsonValue);
        producer.send(record);
        producer.flush();
    }
}
