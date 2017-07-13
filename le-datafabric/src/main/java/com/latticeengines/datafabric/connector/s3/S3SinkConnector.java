package com.latticeengines.datafabric.connector.s3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * S3SinkConnector flushes a Kafka topic to a S3 path in chunks, it will be
 * extended to Snowflake or Reshift connector.
 */
public class S3SinkConnector extends Connector {

    private static final Logger log = LoggerFactory.getLogger(S3SinkConnector.class);
    private Map<String, String> configProperties;

    @Override
    public String version() {
        return S3SinkConstants.VERSION;
    }

    @Override
    public void start(Map<String, String> props) throws ConnectException {
        log.info("Start " + this.getClass().getSimpleName());
        try {
            configProperties = props;
            S3SinkConfig config = new S3SinkConfig(props);
            Boolean cleanup = config.getProperty(S3SinkConfig.S3_CLEANUP, Boolean.class);
            if (cleanup) {
                S3Writer s3Writer = new S3Writer(config);
                s3Writer.initialize();
            }
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start S3SinkConnector due to configuration error", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return S3SinkTask.class;
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
        log.info("Stop " + this.getClass().getSimpleName());
    }

    @Override
    public ConfigDef config() {
        return S3SinkConfig.getConfig();
    }

}
