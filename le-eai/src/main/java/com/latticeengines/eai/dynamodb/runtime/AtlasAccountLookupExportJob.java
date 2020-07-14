package com.latticeengines.eai.dynamodb.runtime;

import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_IDS;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_TTL;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ATLAS_TENANT;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_AWS_REGION;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_AWS_SECRET_KEY_ENCRYPTED;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_EXPORT_VERSION;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ENDPOINT;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_TABLE_NAME;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.eai.runtime.mapreduce.AvroExportJob;
import com.latticeengines.yarn.exposed.client.mapreduce.MapReduceCustomizationRegistry;

public class AtlasAccountLookupExportJob extends AvroExportJob {

    public static final String DYNAMO_EXPORT_JOB_TYPE = "atlasAccountLookupExportJob";

    private int numMappers;

    public AtlasAccountLookupExportJob(Configuration config) {
        super(config);
    }

    public AtlasAccountLookupExportJob(Configuration config, //
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
        return AtlasAccountLookupExportMapper.class;
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
        int numMappers = Integer.parseInt(properties.getProperty(ExportProperty.NUM_MAPPERS, "1"));
        setNumMappers(numMappers);
        super.customize(mrJob, properties);
        Configuration config = mrJob.getConfiguration();
        config.set(CONFIG_ATLAS_TENANT, (String) properties.get(CONFIG_ATLAS_TENANT));
        config.set(CONFIG_TABLE_NAME, (String) properties.get(CONFIG_TABLE_NAME));
        config.set(CONFIG_ATLAS_LOOKUP_IDS, (String) properties.get(CONFIG_ATLAS_LOOKUP_IDS));
        config.set(CONFIG_EXPORT_VERSION, (String) properties.get(CONFIG_EXPORT_VERSION));
        config.set(CONFIG_ATLAS_LOOKUP_TTL, (String) properties.get(CONFIG_ATLAS_LOOKUP_TTL));

        config.set(CONFIG_ENDPOINT, (String) properties.get(CONFIG_ENDPOINT));
        config.set(CONFIG_AWS_REGION, (String) properties.get(CONFIG_AWS_REGION));
        config.set(CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED, (String) properties.get(CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED));
        config.set(CONFIG_AWS_SECRET_KEY_ENCRYPTED, (String) properties.get(CONFIG_AWS_SECRET_KEY_ENCRYPTED));
    }
}
