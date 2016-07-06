package com.latticeengines.datafabric.connector.kafka;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

public class KafkaSinkConnectorConfig extends AbstractConfig {

    // Kafka Group
    public static final String KAFKA_BROKERS_CONFIG = "kafka.brokers";
    private static final String KAFKA_BROKERS_DOC =
        "The kafka cluster to sink records";
    public static final String KAFKA_BROKERS_DEFAULT = "localhost:9092";
    private static final String KAFKA_BROKERS_DISPLAY = "Kafka brokers";

    public static final String KAFKA_ZKCONNECT_CONFIG = "kafka.zkConnect";
    private static final String KAFKA_ZKCONNECT_DOC =
        "The zookeeper servers for sink kafka cluster";
    public static final String KAFKA_ZKCONNECT_DEFAULT = "localhost:2181";
    private static final String KAFKA_ZKCONNECT_DISPLAY = "Kafka brokers";

    public static final String KAFKA_SCHEMAREG_CONFIG = "kafka.schemaReg";
    private static final String KAFKA_SCHEMAREG_DOC =
        "The schema registry url for sink kafka cluster";
    public static final String KAFKA_SCHEMAREG_DEFAULT = "http://localhost:8081";
    private static final String KAFKA_SCHEMAREG_DISPLAY = "Kafka schema registry url";

    public static final String KAFKA_STACK_CONFIG = "kafka.stack";
    private static final String KAFKA_STACK_DOC =
        "The lattice stack for sink kafka cluster";
    public static final String KAFKA_STACK_DEFAULT = "";
    private static final String KAFKA_STACK_DISPLAY = "lattice stack";

    public static final String KAFKA_ENVIRONMENT_CONFIG = "kafka.environment";
    private static final String KAFKA_ENVIRONMENT_DOC =
            "The lattice environment for sink kafka cluster";
    public static final String KAFKA_ENVIRONMENT_DEFAULT = "dev";
    private static final String KAFKA_ENVIRONMENT_DISPLAY = "lattice environment";

    public static final String KAFKA_TOPIC_CONFIG = "kafka.topic";
    private static final String KAFKA_TOPIC_DOC =
        "The topic to sink records";
    public static final String KAFKA_TOPIC_DEFAULT = "Unknown";
    private static final String KAFKA_TOPIC_DISPLAY = "Kafka topic";

    public static final String KAFKA_SCOPE_CONFIG = "kafka.scope";
    private static final String KAFKA_SCOPE_DOC =
        "The privacy scope of the topic: private, environment_private, public";
    public static final String KAFKA_SCOPE_DEFAULT = "private";
    private static final String KAFKA_SCOPE_DISPLAY = "Kafka topic scope";

    public static final String KAFKA_RECORD_CONFIG = "kafka.record";
    private static final String KAFKA_RECORD_DOC =
        "The type of records to be sinked";
    public static final String KAFKA_RECORD_DEFAULT = "ScoreRecord";
    private static final String KAFKA_RECORD_DISPLAY = "Kafka Record";

    public static final String FLUSH_SIZE_CONFIG = "flush.size";
    private static final String FLUSH_SIZE_DOC =
        "Number of records written to HDFS before invoking file commits.";
    private static final String FLUSH_SIZE_DISPLAY = "Flush Size";

    public static final String RETRY_BACKOFF_CONFIG = "retry.backoff.ms";
    private static final String RETRY_BACKOFF_DOC =
        "The retry backoff in milliseconds. This config is used to "
        + "notify Kafka connect to retry delivering a message batch or performing recovery in case "
        + "of transient exceptions.";
    public static final long RETRY_BACKOFF_DEFAULT = 5000L;
    private static final String RETRY_BACKOFF_DISPLAY = "Retry Backoff (ms)";

    public static final String SHUTDOWN_TIMEOUT_CONFIG = "shutdown.timeout.ms";
    private static final String SHUTDOWN_TIMEOUT_DOC =
        "Clean shutdown timeout. This makes sure that asynchronous Hive metastore updates are "
        + "completed during connector shutdown.";
    private static final long SHUTDOWN_TIMEOUT_DEFAULT = 3000L;
    private static final String SHUTDOWN_TIMEOUT_DISPLAY = "Shutdown Timeout (ms)";

    // Schema group
    public static final String SCHEMA_CACHE_SIZE_CONFIG = "schema.cache.size";
    private static final String SCHEMA_CACHE_SIZE_DOC =
        "The size of the schema cache used in the Avro converter.";
    public static final int SCHEMA_CACHE_SIZE_DEFAULT = 1000;
    private static final String SCHEMA_CACHE_SIZE_DISPLAY = "Schema Cache Size";

    public static final String KAFKA_GROUP = "KAFKA";
    public static final String SCHEMA_GROUP = "Schema";
    public static final String CONNECTOR_GROUP = "Connector";
    public static final String INTERNAL_GROUP = "Internal";

    private static ConfigDef config = new ConfigDef();

    static {

        // Define Redis configuration group.
        config.define(KAFKA_BROKERS_CONFIG, Type.STRING, KAFKA_BROKERS_DEFAULT, Importance.HIGH, KAFKA_BROKERS_DOC, KAFKA_GROUP, 2, Width.MEDIUM, KAFKA_BROKERS_DISPLAY)
              .define(KAFKA_ZKCONNECT_CONFIG, Type.STRING, KAFKA_ZKCONNECT_DEFAULT, Importance.HIGH, KAFKA_ZKCONNECT_DOC, KAFKA_GROUP, 4, Width.SHORT, KAFKA_ZKCONNECT_DISPLAY) //
              .define(KAFKA_SCHEMAREG_CONFIG, Type.STRING, KAFKA_SCHEMAREG_DEFAULT, Importance.HIGH, KAFKA_SCHEMAREG_DOC, KAFKA_GROUP, 4, Width.SHORT, KAFKA_SCHEMAREG_DISPLAY) //
              .define(KAFKA_STACK_CONFIG, Type.STRING, KAFKA_STACK_DEFAULT, Importance.HIGH, KAFKA_STACK_DOC, KAFKA_GROUP, 4, Width.SHORT, KAFKA_STACK_DISPLAY) //
              .define(KAFKA_ENVIRONMENT_CONFIG, Type.STRING, KAFKA_ENVIRONMENT_DEFAULT, Importance.HIGH, KAFKA_ENVIRONMENT_DOC, KAFKA_GROUP, 4, Width.SHORT, KAFKA_ENVIRONMENT_DISPLAY) //
              .define(KAFKA_TOPIC_CONFIG, Type.STRING, KAFKA_TOPIC_DEFAULT, Importance.HIGH, KAFKA_TOPIC_DOC, KAFKA_GROUP, 4, Width.SHORT, KAFKA_TOPIC_DISPLAY) //
              .define(KAFKA_SCOPE_CONFIG, Type.STRING, KAFKA_SCOPE_DEFAULT, Importance.HIGH, KAFKA_SCOPE_DOC, KAFKA_GROUP, 4, Width.SHORT, KAFKA_SCOPE_DISPLAY) //
              .define(KAFKA_RECORD_CONFIG, Type.STRING, KAFKA_RECORD_DEFAULT, Importance.HIGH, KAFKA_RECORD_DOC, KAFKA_GROUP, 4, Width.SHORT, KAFKA_RECORD_DISPLAY);

        // Define Schema configuration group
        config.define(SCHEMA_CACHE_SIZE_CONFIG, Type.INT, SCHEMA_CACHE_SIZE_DEFAULT, Importance.LOW, SCHEMA_CACHE_SIZE_DOC, SCHEMA_GROUP, 2, Width.SHORT, SCHEMA_CACHE_SIZE_DISPLAY);

        // Define Connector configuration group
        config.define(FLUSH_SIZE_CONFIG, Type.INT, Importance.HIGH, FLUSH_SIZE_DOC, CONNECTOR_GROUP, 1, Width.SHORT, FLUSH_SIZE_DISPLAY)
            .define(RETRY_BACKOFF_CONFIG, Type.LONG, RETRY_BACKOFF_DEFAULT, Importance.LOW, RETRY_BACKOFF_DOC, CONNECTOR_GROUP, 3, Width.SHORT, RETRY_BACKOFF_DISPLAY)
            .define(SHUTDOWN_TIMEOUT_CONFIG, Type.LONG, SHUTDOWN_TIMEOUT_DEFAULT, Importance.MEDIUM, SHUTDOWN_TIMEOUT_DOC, CONNECTOR_GROUP, 4, Width.SHORT, SHUTDOWN_TIMEOUT_DISPLAY);

    }

    private static boolean classNameEquals(String className, Class<?> clazz) {
        return className.equals(clazz.getSimpleName()) || className.equals(clazz.getCanonicalName());
    }

    public static ConfigDef getConfig() {
        return config;
    }

    public KafkaSinkConnectorConfig(Map<String, String> props) {
        super(config, props);
    }

    public static void main(String[] args) {
        System.out.println(config.toRst());
    }
}
