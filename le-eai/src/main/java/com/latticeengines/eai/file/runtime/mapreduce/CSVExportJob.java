package com.latticeengines.eai.file.runtime.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.eai.runtime.mapreduce.AvroExportJob;

public class CSVExportJob extends AvroExportJob {

    public static final String CSV_EXPORT_JOB_TYPE = "eaiCSVExportJob";

    public CSVExportJob(Configuration config) {
        super(config);
    }

    public CSVExportJob(Configuration config, //
            MapReduceCustomizationRegistry mapReduceCustomizationRegistry, //
            VersionManager versionManager) {
        super(config, mapReduceCustomizationRegistry, versionManager);
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

}
