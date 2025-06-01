package com.dvwy.day3.transaction;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class TransactionPipeline  {
    public static void main(String[] args) {

        String topic = "test-transaction";
        String groupId = "consumer-group-1";
        String transactionalId = "tx-producer-1";

        // Producer 설정
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        producer.initTransactions();

        // Consumer 설정
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            if (records.isEmpty()) continue;

            producer.beginTransaction();

            for (ConsumerRecord<String, String> record : records) {
                ProducerRecord<String, String> out = new ProducerRecord<>("test-transaction-output", record.key(), record.value());
                producer.send(out);
            }

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
            }

            producer.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(groupId));
            producer.commitTransaction();
        }
    }
}