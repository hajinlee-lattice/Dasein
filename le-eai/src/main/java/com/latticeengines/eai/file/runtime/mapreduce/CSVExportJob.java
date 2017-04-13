package com.latticeengines.eai.file.runtime.mapreduce;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import com.latticeengines.dataplatform.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.eai.runtime.mapreduce.AvroExportJob;

public class CSVExportJob extends AvroExportJob {

    public static final String CSV_EXPORT_JOB_TYPE = "eaiCSVExportJob";

    public CSVExportJob(Configuration config) {
        super(config);
    }

    public CSVExportJob(Configuration config, //
            MapReduceCustomizationRegistry mapReduceCustomizationRegistry) {
        super(config, mapReduceCustomizationRegistry);
    }

    @Override
    public String getJobType() {
        return CSV_EXPORT_JOB_TYPE;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Class<? extends Mapper> getMapperClass() {
        return CSVExportMapper.class;
    }

    @Override
    protected int getNumMappers() {
        return 1;
    }

    @Override
    public void customize(Job mrJob, Properties properties) {
        super.customize(mrJob, properties);
        Configuration config = mrJob.getConfiguration();
        String exportUsingDisplayName = properties.getProperty("eai.export.displayname");
        config.setBoolean("eai.export.displayname", Boolean.valueOf(exportUsingDisplayName));
    }
}
