package dvwy.day2.connector.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class SingleFileSourceConnectorConfig extends AbstractConfig {


    public static ConfigDef CONFIG = null;

    public SingleFileSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}
