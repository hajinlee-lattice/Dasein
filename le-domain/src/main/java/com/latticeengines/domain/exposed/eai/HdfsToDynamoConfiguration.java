package com.latticeengines.domain.exposed.eai;

public class HdfsToDynamoConfiguration extends ExportConfiguration {

    public static final String CONFIG_RECORD_TYPE = "eai.export.dynamo.record.type";
    public static final String CONFIG_REPOSITORY = "eai.export.dynamo.repository";
    public static final String CONFIG_ENTITY_CLASS_NAME = "eai.export.dynamo.entity.class";

    public static final String CONFIG_KEY_PREFIX = "eai.export.dynamo.key_prefix";
    public static final String CONFIG_PARTITION_KEY = "eai.export.dynamo.partition.key";
    public static final String CONFIG_SORT_KEY = "eai.export.dynamo.sort.key";

    public static final String CONFIG_ENDPOINT = "eai.export.dynamo.endpoint";
    public static final String CONFIG_AWS_REGION = "eai.export.aws.region";
    public static final String CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED = "eai.export.aws.access.key.id";
    public static final String CONFIG_AWS_SECRET_KEY_ENCRYPTED = "eai.export.aws.secret.key";

    public static final String CONFIG_TABLE_NAME = "eai.export.dynamo.table.name";
    public static final String CONFIG_ATLAS_TENANT = "eai.export.dynamo.atlas.tenant";
    public static final String CONFIG_ATLAS_LOOKUP_IDS = "eai.export.dynamo.atlas.lookup.ids";
}
