package com.latticeengines.eai.dynamodb.runtime;

import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_AWS_REGION;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_AWS_SECRET_KEY_ENCRYPTED;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ENDPOINT;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ENTITY_CLASS_NAME;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_PARTITION_KEY;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_RECORD_TYPE;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_REPOSITORY;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_SORT_KEY;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_KEY_PREFIX;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.eai.runtime.mapreduce.AvroExportJob;
import com.latticeengines.yarn.exposed.client.mapreduce.MapReduceCustomizationRegistry;

public class DynamoExportJob extends AvroExportJob {

    public static final String DYNAMO_EXPORT_JOB_TYPE = "eaiDynamoExportJob";

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
        config.set(CONFIG_AWS_REGION, (String) properties.get(CONFIG_AWS_REGION));
        config.set(CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED, (String) properties.get(CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED));
        config.set(CONFIG_AWS_SECRET_KEY_ENCRYPTED, (String) properties.get(CONFIG_AWS_SECRET_KEY_ENCRYPTED));

        config.set(CONFIG_KEY_PREFIX, (String) properties.get(CONFIG_KEY_PREFIX));
        config.set(CONFIG_PARTITION_KEY, (String) properties.get(CONFIG_PARTITION_KEY));
        config.set(CONFIG_SORT_KEY, (String) properties.get(CONFIG_SORT_KEY));
    }
}
