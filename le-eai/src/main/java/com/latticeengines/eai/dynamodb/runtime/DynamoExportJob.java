package com.latticeengines.eai.dynamodb.runtime;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import com.latticeengines.dataplatform.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.eai.runtime.mapreduce.AvroExportJob;

public class DynamoExportJob extends AvroExportJob {

    public static final String DYNAMO_EXPORT_JOB_TYPE = "eaiDynamoExportJob";

    public static final String CONFIG_RECORD_TYPE = "eai.export.dynamo.record.type";
    public static final String CONFIG_REPOSITORY = "eai.export.dynamo.repository";
    public static final String CONFIG_ENTITY_CLASS_NAME = "eai.export.dynamo.entity.class";

    public static final String CONFIG_ENDPOINT = "eai.export.dynamo.endpoint";
    public static final String CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED = "eai.export.aws.access.key.id";
    public static final String CONFIG_AWS_SECRET_KEY_ENCRYPTED = "eai.export.aws.secret.key";

    private int numMappers;

    public DynamoExportJob(Configuration config) {
        super(config);
    }

    public DynamoExportJob(Configuration config, //
            MapReduceCustomizationRegistry mapReduceCustomizationRegistry) {
        super(config, mapReduceCustomizationRegistry);
    }

    @Override
    public String getJobType() {
        return DYNAMO_EXPORT_JOB_TYPE;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Class<? extends Mapper> getMapperClass() {
        return DynamoExportMapper.class;
    }

    @Override
    protected int getNumMappers() {
        return numMappers;
    }

    private void setNumMappers(int numMappers) {
        this.numMappers = numMappers;
    }

    @Override
    public void customize(Job mrJob, Properties properties) {
        int numMappers = Integer.valueOf(properties.getProperty(ExportProperty.NUM_MAPPERS, "1"));
        setNumMappers(numMappers);
        super.customize(mrJob, properties);
        Configuration config = mrJob.getConfiguration();
        config.set(CONFIG_RECORD_TYPE, (String) properties.get(CONFIG_RECORD_TYPE));
        config.set(CONFIG_REPOSITORY, (String) properties.get(CONFIG_REPOSITORY));
        config.set(CONFIG_ENTITY_CLASS_NAME, (String) properties.get(CONFIG_ENTITY_CLASS_NAME));
        config.set(CONFIG_ENDPOINT, (String) properties.get(CONFIG_ENDPOINT));
        config.set(CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED, (String) properties.get(CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED));
        config.set(CONFIG_AWS_SECRET_KEY_ENCRYPTED, (String) properties.get(CONFIG_AWS_SECRET_KEY_ENCRYPTED));
    }
}
