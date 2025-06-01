package com.dvwy.day2.steams.dsl.kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class KStreamMap {

    private static String APPLICATION_NAME = "streams-map-application";
    private static String BOOTSTRAP_SERVERS = "[::1]:9092";
    private static String STREAM_LOG = "test";
    private static String OUTPUT_LOG = "test2";


    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> log = builder.stream(STREAM_LOG);
        log.map(((key, value) -> new KeyValue<>("key-" + key, value))).to("value"+ OUTPUT_LOG);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}