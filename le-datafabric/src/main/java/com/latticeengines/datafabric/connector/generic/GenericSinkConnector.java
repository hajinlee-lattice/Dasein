package com.latticeengines.datafabric.connector.generic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.CamilleConfiguration;
import com.latticeengines.camille.exposed.CamilleEnvironment;

public class GenericSinkConnector extends Connector {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(GenericSinkConnector.class);
    private Map<String, String> configProperties;

    @SuppressWarnings("unused")
    private GenericSinkConnectorConfig config;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) throws ConnectException {
        try {
            configProperties = props;
            config = new GenericSinkConnectorConfig(props);
        } catch (ConfigException e) {
            log.error("Generic Connector failed!", e);
            throw new RetriableException("Couldn't start GenericSinkConnector due to configuration error", e);
        }
        startCamille(config);
    }

    private void startCamille(GenericSinkConnectorConfig config) {
        try {
            CamilleConfiguration camilleConf = new CamilleConfiguration();
            camilleConf.setConnectionString(
                    config.getProperty(GenericSinkConnectorConfig.CAMILLE_ZK_CONNECTION, String.class));
            camilleConf.setPodId(config.getProperty(GenericSinkConnectorConfig.CAMILLE_ZK_POD_ID, String.class));
            CamilleEnvironment.start(CamilleEnvironment.Mode.RUNTIME, camilleConf);
        } catch (Exception e) {
            throw new RuntimeException("Cannot bootstrap camille environment.", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GenericSinkTask.class;
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
        return GenericSinkConnectorConfig.getConfig();
    }

}
