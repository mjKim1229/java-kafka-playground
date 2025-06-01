package dvwy.day2.connector.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class SingleFileSinkTask extends SinkTask {
    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
    }

    @Override
    public void put(Collection<SinkRecord> records) {
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void stop() {
    }
}