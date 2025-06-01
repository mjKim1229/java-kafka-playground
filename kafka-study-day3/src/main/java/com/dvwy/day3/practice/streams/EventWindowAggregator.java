package com.dvwy.day3.practice.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

public class EventWindowAggregator {
    private static String APPLICATION_NAME = "window-aggregator-application";
    private static String BOOTSTRAP_SERVERS = "[::1]:9092";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 3000);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/" + UUID.randomUUID());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> webRawLogStream = builder.stream("web-raw-log");
        KStream<Windowed<String>, Long> countWindow = webRawLogStream.groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)).grace(Duration.ofSeconds(1)))
                .count()
                .toStream();

        countWindow.map((windowKey, count) -> {
            String user = windowKey.key();
            String windowInfo = "[" + windowKey.window().startTime() + "-" + windowKey.window().endTime() + "]";
            String outputJson = String.format("{\"user\":\"%s\", \"count\":\"%d\"}", user, count);
            return new KeyValue<>(user + windowInfo, outputJson);
        }).to("web-aggregated");


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}