package com.latticeengines.datafabric.connector.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaSinkConnector is a Kafka Connect Connector implementation that ingest data from one Kafka cluster
 * to another Kafka cluster.
 */
public class KafkaSinkConnector extends Connector {

    private static final Logger log = LoggerFactory.getLogger(KafkaSinkConnector.class);
    private Map<String, String> configProperties;
    private KafkaSinkConnectorConfig config;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) throws ConnectException {
        try {
            configProperties = props;
            config = new KafkaSinkConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start KafkaSinkConnector due to configuration error", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KafkaSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() throws ConnectException {

    }

    @Override
    public ConfigDef config() {
        return KafkaSinkConnectorConfig.getConfig();
    }

}
