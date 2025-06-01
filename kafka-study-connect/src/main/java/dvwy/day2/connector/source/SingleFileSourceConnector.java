package dvwy.day2.connector.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleFileSourceConnector extends SourceConnector {

    private final Logger logger = LoggerFactory.getLogger(SingleFileSourceConnector.class);


    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SingleFileSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return null;
    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void stop() {
    }
}
