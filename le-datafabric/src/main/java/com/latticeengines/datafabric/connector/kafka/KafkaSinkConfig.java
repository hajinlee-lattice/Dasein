package com.latticeengines.datafabric.connector.kafka;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import com.latticeengines.datafabric.connector.ConnectorConfiguration;
import com.latticeengines.datafabric.connector.WorkerProperty;

public class KafkaSinkConfig extends ConnectorConfiguration {

    private static ConfigDef config;

    public static ConfigDef getConfig() {
        return config;
    }

    // Kafka Group
    static final WorkerProperty<String> KAFKA_BROKERS = new WorkerProperty<String>("kafka.brokers",
            "The kafka cluster to sink records", "Kafka brokers").setDefaultValue("localhost:9092");

    static final WorkerProperty<String> KAFKA_ZKCONNECT = new WorkerProperty<String>("kafka.zkConnect",
            "The zookeeper servers for sink kafka cluster", "Kafka brokers").setDefaultValue("localhost:2181");

    static final WorkerProperty<String> KAFKA_SCHEMAREG = new WorkerProperty<String>("kafka.schemaReg",
            "The schema registry url for sink kafka cluster", "Kafka schema registry url")
                    .setDefaultValue("http://localhost:9022");

    static final WorkerProperty<String> KAFKA_RECORD = new WorkerProperty<>("kafka.record",
            "The avro record type to be sunk", "Kafka record");

    static final WorkerProperty<String> KAFKA_ENVIRONMENT = new WorkerProperty<>("kafka.environment",
            "The lattice environment for sink kafka cluster", "lattice environment");

    static final WorkerProperty<String> KAFKA_STACK = new WorkerProperty<>("kafka.stack",
            "The lattice stack for sink kafka cluster", "lattice stack");

    static final WorkerProperty<String> KAFKA_LOGICAL_TOPIC = new WorkerProperty<>("kafka.logical.topic",
            "The topic to sink records", "Logical topic of sink kafka.");

    static final WorkerProperty<String> KAFKA_TOPIC_SCOPE = new WorkerProperty<>("kafka.topic.scope",
            "The privacy scope of the topic: private, environment_private, public", "Kafka topic scope");

    static {
        String KAFKA_GROUP = "KAFKA";

        initialize();
        addGroup(KAFKA_GROUP);

        addPropertyToGroup(KAFKA_BROKERS, String.class, KAFKA_GROUP);
        addPropertyToGroup(KAFKA_ZKCONNECT, String.class, KAFKA_GROUP);
        addPropertyToGroup(KAFKA_SCHEMAREG, String.class, KAFKA_GROUP);
        addPropertyToGroup(KAFKA_RECORD, String.class, KAFKA_GROUP);
        addPropertyToGroup(KAFKA_ENVIRONMENT, String.class, KAFKA_GROUP);
        addPropertyToGroup(KAFKA_STACK, String.class, KAFKA_GROUP);
        addPropertyToGroup(KAFKA_LOGICAL_TOPIC, String.class, KAFKA_GROUP);
        addPropertyToGroup(KAFKA_TOPIC_SCOPE, String.class, KAFKA_GROUP);

        config = tmpConfig.get();
    }

    KafkaSinkConfig(Map<String, String> props) {
        super(config, props);
    }
}
