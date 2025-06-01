package com.dvwy.day2.steams.processor.filter;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
public class SimpleProcessor {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "filter-processor-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new Topology();
        topology.addSource("Source", "test");
        topology.addProcessor("FilterProcessor", FilterProcessor::new, "Source");
        topology.addSink("Sink", "output", "FilterProcessor");

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
