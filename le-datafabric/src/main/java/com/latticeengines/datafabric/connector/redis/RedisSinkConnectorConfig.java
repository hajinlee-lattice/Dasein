package com.latticeengines.datafabric.connector.redis;

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RedisSinkConnectorConfig extends AbstractConfig {

    // Redis Group
    public static final String REDIS_SERVERS_CONFIG = "redis.servers";
    private static final String REDIS_SERVERS_DOC =
        "The desination Redis cluster to sink records";
    public static final String REDIS_SERVERS_DEFAULT = "localhost";
    private static final String REDIS_SERVERS_DISPLAY = "Redis Servers";

    public static final String REDIS_PORT_CONFIG = "redis.port";
    private static final String REDIS_PORT_DOC =
        "The desination port of Redis cluster";
    public static final int REDIS_PORT_DEFAULT = 6379;
    private static final String REDIS_PORT_DISPLAY = "Redis port";

    public static final String REDIS_HA_CONFIG = "redis.ha";
    private static final String REDIS_HA_DOC =
        "Is Ha enabled for redis cluster";
    public static final boolean REDIS_HA_DEFAULT = false;
    private static final String REDIS_HA_DISPLAY = "Redis HA Enabled";

    public static final String REDIS_MASTER_CONFIG = "redis.master";
    private static final String REDIS_MASTER_DOC =
        "Master node of  redis cluster";
    public static final String REDIS_MASTER_DEFAULT = "mymaster";
    private static final String REDIS_MASTER_DISPLAY = "Redis cluster master";

    public static final String REDIS_REPO_CONFIG = "redis.repo";
    private static final String REDIS_REPO_DOC =
        "The repository inside Redis data store";
    public static final String REDIS_REPO_DEFAULT = "production";
    private static final String REDIS_REPO_DISPLAY = "Redis Repository";

    public static final String REDIS_RECORD_CONFIG = "redis.record";
    private static final String REDIS_RECORD_DOC =
        "The type of records to be sinked";
    public static final String REDIS_RECORD_DEFAULT = "ScoreRecord";
    private static final String REDIS_RECORD_DISPLAY = "Redis Record";

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

    public static final String REDIS_GROUP = "REDIS";
    public static final String SCHEMA_GROUP = "Schema";
    public static final String CONNECTOR_GROUP = "Connector";
    public static final String INTERNAL_GROUP = "Internal";

    private static ConfigDef config = new ConfigDef();

    static {

        // Define Redis configuration group.
        config.define(REDIS_SERVERS_CONFIG, Type.STRING, REDIS_SERVERS_DEFAULT, Importance.HIGH, REDIS_SERVERS_DOC, REDIS_GROUP, 2, Width.MEDIUM, REDIS_SERVERS_DISPLAY)
              .define(REDIS_PORT_CONFIG, Type.INT, REDIS_PORT_DEFAULT, Importance.HIGH, REDIS_PORT_DOC, REDIS_GROUP, 4, Width.SHORT, REDIS_PORT_DISPLAY)
              .define(REDIS_HA_CONFIG, Type.BOOLEAN, REDIS_HA_DEFAULT, Importance.HIGH, REDIS_HA_DOC, REDIS_GROUP, 4, Width.SHORT, REDIS_HA_DISPLAY)
              .define(REDIS_MASTER_CONFIG, Type.STRING, REDIS_MASTER_DEFAULT, Importance.HIGH, REDIS_MASTER_DOC, REDIS_GROUP, 4, Width.SHORT, REDIS_MASTER_DISPLAY)
              .define(REDIS_REPO_CONFIG, Type.STRING, REDIS_REPO_DEFAULT, Importance.HIGH, REDIS_REPO_DOC, REDIS_GROUP, 4, Width.SHORT, REDIS_REPO_DISPLAY)
              .define(REDIS_RECORD_CONFIG, Type.STRING, REDIS_RECORD_DEFAULT, Importance.HIGH, REDIS_RECORD_DOC, REDIS_GROUP, 4, Width.SHORT, REDIS_RECORD_DISPLAY);

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

    public RedisSinkConnectorConfig(Map<String, String> props) {
        super(config, props);
    }

    public static void main(String[] args) {
        System.out.println(config.toRst());
    }
}
