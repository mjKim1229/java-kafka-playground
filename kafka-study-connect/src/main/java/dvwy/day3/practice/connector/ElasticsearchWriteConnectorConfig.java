package dvwy.day3.practice.connector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ElasticsearchWriteConnectorConfig extends AbstractConfig {


    public static final ConfigDef CONFIG = null;

    public ElasticsearchWriteConnectorConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    public static ConfigDef config() {
        return CONFIG;
    }
}
