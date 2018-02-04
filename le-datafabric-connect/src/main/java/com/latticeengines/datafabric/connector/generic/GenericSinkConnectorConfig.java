package com.latticeengines.datafabric.connector.generic;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import com.latticeengines.datafabric.connector.ConnectorConfiguration;
import com.latticeengines.datafabric.connector.WorkerProperty;

public class GenericSinkConnectorConfig extends ConnectorConfiguration {

    private static final String GENERIC_COMMON_GROUP = "GENERIC_COMMON";
    private static final String GENERIC_HDFS_GROUP = "GENERIC_HDFS";
    private static final String GENERIC_DYNAMO_GROUP = "GENERIC_DYNAMO";
    private static ConfigDef config;

    public static ConfigDef getConfig() {
        return config;
    }

    // common
    static final WorkerProperty<String> STACK = new WorkerProperty<String>("datafabric.message.stack",
            "The stack name used in Zookeeper", "The stack used in Zookeeper").setDefaultValue("global");
    static final WorkerProperty<String> KAFKA_ZKCONNECT = new WorkerProperty<String>("kafka.zkConnect",
            "The zookeeper servers for sink generic cluster", "Zookeeper servers").setDefaultValue("localhost:2181/kafka");
    static final WorkerProperty<String> REPOSITORIES = new WorkerProperty<String>("datafabric.connect.repositories",
            "Supported stores .e.g HDFS, REDIS, DYNAMO", "stores").setDefaultValue("HDFS;DYNAMO");

    // camille
    static final WorkerProperty<String> CAMILLE_ZK_CONNECTION = new WorkerProperty<String>("camille.zk.connectionString",
            "The zk connection for camille", "ZK connection string for Camille").setDefaultValue("localhost:2181");
    static final WorkerProperty<String> CAMILLE_ZK_POD_ID = new WorkerProperty<String>("camille.zk.pod.id",
            "Cammille pod id", "pod id").setDefaultValue("Default");

    // HDFS
    static final WorkerProperty<String> HADOOP_CONF_DIR = new WorkerProperty<String>("hadoop.conf.dir",
            "The Hadoop configuration directory.", "Hadoop Configuration Directory").setDefaultValue("");
    static final WorkerProperty<String> HDFS_BASE_DIR = new WorkerProperty<String>("hdfs.base.dir",
            "HDFS base directory to save files.", "HDFS base Directory").setDefaultValue("/Pods/Default/Services/PropData/Sources");

    // DYNAMO
    static final WorkerProperty<String> ACCESS_KEY = new WorkerProperty<String>("aws.default.access.key",
            "AWS access key", "AWS access key").setDefaultValue("AKIAJYGRJBKAXQAV5OXQ");
    static final WorkerProperty<String> SECRET_KEY = new WorkerProperty<String>("aws.default.secret.key",
            "AWS secret key", "AWS secret key").setDefaultValue("Kw4HndU744WxSegVdAZ+hgiL3ogupKIkI1Ce8Cjj");

    static {
        initialize();
        addGroup(GENERIC_COMMON_GROUP);
        addPropertyToGroup(STACK, String.class, GENERIC_COMMON_GROUP);
        addPropertyToGroup(KAFKA_ZKCONNECT, String.class, GENERIC_COMMON_GROUP);
        addPropertyToGroup(REPOSITORIES, String.class, GENERIC_COMMON_GROUP);
        addPropertyToGroup(CAMILLE_ZK_CONNECTION, String.class, GENERIC_COMMON_GROUP);
        addPropertyToGroup(CAMILLE_ZK_POD_ID, String.class, GENERIC_COMMON_GROUP);

        addGroup(GENERIC_HDFS_GROUP);
        addPropertyToGroup(HADOOP_CONF_DIR, String.class, GENERIC_HDFS_GROUP);
        addPropertyToGroup(HDFS_BASE_DIR, String.class, GENERIC_HDFS_GROUP);

        addGroup(GENERIC_DYNAMO_GROUP);
        addPropertyToGroup(ACCESS_KEY, String.class, GENERIC_DYNAMO_GROUP);
        addPropertyToGroup(SECRET_KEY, String.class, GENERIC_DYNAMO_GROUP);

        config = tmpConfig.get();
    }

    GenericSinkConnectorConfig(Map<String, String> props) {
        super(config, props);
    }
}
