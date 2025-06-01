package dvwy.day3.practice.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticsearchWriteConnector extends SinkConnector {

    private Map<String, String> configProps;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ElasticsearchWriteTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void stop() {
        // 리소스 정리 없음
    }

    @Override
    public ConfigDef config() {
        return ElasticsearchWriteConnectorConfig.config();
    }
}
