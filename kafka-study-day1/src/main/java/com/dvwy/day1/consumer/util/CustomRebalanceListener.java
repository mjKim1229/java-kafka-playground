package com.dvwy.day1.consumer.util;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class CustomRebalanceListener implements ConsumerRebalanceListener {
    private final static Logger logger = LoggerFactory.getLogger(CustomRebalanceListener.class);

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    }
}