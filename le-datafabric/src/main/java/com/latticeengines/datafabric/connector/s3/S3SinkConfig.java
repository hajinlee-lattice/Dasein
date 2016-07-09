package com.latticeengines.datafabric.connector.s3;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import com.latticeengines.datafabric.connector.ConnectorConfiguration;
import com.latticeengines.datafabric.connector.WorkerProperty;

public class S3SinkConfig extends ConnectorConfiguration {

    private static ConfigDef config;

    public static ConfigDef getConfig() {
        return config;
    }

    // S3 Group
    static final WorkerProperty<String> S3_BUCKET = new WorkerProperty<>("s3.bucket", "S3 bucket", "S3 Bucket");
    static final WorkerProperty<String> S3_PREFIX = new WorkerProperty<>("s3.prefix",
            "prefix in S3 key. CAUTION: all objects under prefix will be cleaned up when connector startup",
            "S3 Prefix");
    static final WorkerProperty<Boolean> S3_CLEANUP = new WorkerProperty<Boolean>("s3.cleanup",
            "Cleanup the S3 destination upon connector startup.",
            "Cleanup S3 destination").setDefaultValue(Boolean.FALSE);
    static final WorkerProperty<String> AWS_ACCESS_KEY_ID = new WorkerProperty<String>("aws.access.key.id",
            "aws access key id. must also specify aws.secret.key", "AWS AccessKey ID") //
            .setImportance(ConfigDef.Importance.LOW);
    static final WorkerProperty<String> AWS_SECRET_KEY = new WorkerProperty<String>("aws.secret.key",
            "aws secret key. must also specify aws.access.key.id", "AWS Secret Key") //
            .setImportance(ConfigDef.Importance.LOW);

    static {
        String S3_GROUP = "S3";

        initialize();
        addGroup(S3_GROUP);

        addPropertyToGroup(S3_BUCKET, String.class, S3_GROUP);
        addPropertyToGroup(S3_PREFIX, String.class, S3_GROUP);
        addPropertyToGroup(S3_CLEANUP, Boolean.class, S3_GROUP);
        addPropertyToGroup(AWS_ACCESS_KEY_ID, String.class, S3_GROUP);
        addPropertyToGroup(AWS_SECRET_KEY, String.class, S3_GROUP);

        config = tmpConfig.get();
    }

    S3SinkConfig(Map<String, String> props) {
        super(config, props);
    }


    public static void main(String[] args) {
        System.out.println(config.toRst());
    }
}


