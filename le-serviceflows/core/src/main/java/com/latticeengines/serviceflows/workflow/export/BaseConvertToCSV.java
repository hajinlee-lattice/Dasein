package com.latticeengines.serviceflows.workflow.export;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.common.ConvertToCSVJob;

public abstract class BaseConvertToCSV<S extends BaseStepConfiguration>
        extends RunSparkJob<S, ConvertToCSVConfig> {

    protected abstract HdfsDataUnit getInputData();
    protected abstract Map<String, String> getDisplayNameMap();
    protected abstract Map<String, String> getDateAttrFmtMap();
    protected abstract void processResultCSV(String csvGzFilePath);

    @Override
    protected Class<ConvertToCSVJob> getJobClz() {
        return ConvertToCSVJob.class;
    }

    @Override
    protected ConvertToCSVConfig configureJob(S stepConfiguration) {
        ConvertToCSVConfig config = new ConvertToCSVConfig();
        config.setInput(Collections.singletonList(getInputData()));
        config.setDisplayNames(getDisplayNameMap());
        config.setDateAttrsFmt(getDateAttrFmtMap());
        config.setTimeZone(getTimeZone());
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String outputDir = result.getTargets().get(0).getPath();
        String csvGzPath;
        try {
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, outputDir, //
                    (HdfsUtils.HdfsFilenameFilter) filename -> filename.endsWith(".csv.gz"));
            csvGzPath = files.get(0);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + outputDir);
        }
        processResultCSV(csvGzPath);
    }

    protected String getTimeZone() {
        return "UTC";
    }

}
