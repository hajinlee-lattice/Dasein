package com.latticeengines.eai.yarn.runtime;

import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.eai.HdfsToS3Configuration;
import com.latticeengines.domain.exposed.eai.HdfsToSnowflakeConfiguration;
import com.latticeengines.eai.service.impl.redshift.HdfsToRedshiftService;
import com.latticeengines.eai.service.impl.s3.HdfsToS3ExportService;
import com.latticeengines.eai.service.impl.snowflake.HdfsToSnowflakeService;

@Component("exportProcessor")
public class ExportProcessor extends SingleContainerYarnProcessor<ExportConfiguration>
        implements ItemProcessor<ExportConfiguration, String> {

    private static final Log log = LogFactory.getLog(ExportProcessor.class);

    @Autowired
    private HdfsToS3ExportService hdfsToS3ExportService;

    @Autowired
    private HdfsToSnowflakeService hdfsToSnowflakeService;

    @Autowired
    private HdfsToRedshiftService hdfsToRedshiftService;

    @Override
    public String process(ExportConfiguration exportConfig) throws Exception {
        if (exportConfig instanceof HdfsToS3Configuration) {
            invokeS3Upload((HdfsToS3Configuration) exportConfig);
        } else if (exportConfig instanceof HdfsToSnowflakeConfiguration) {
            invokeSnowflakeExport((HdfsToSnowflakeConfiguration) exportConfig);
        } else if (exportConfig instanceof HdfsToRedshiftConfiguration) {
            invokeRedshiftExport((HdfsToRedshiftConfiguration) exportConfig);
        }
        return null;
    }

    private void invokeRedshiftExport(HdfsToRedshiftConfiguration configuration) {
        try {
            hdfsToRedshiftService.uploadToS3(configuration);
        } catch (IOException e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
            throw new RuntimeException(e);
        }
        setProgress(0.6f);
        hdfsToRedshiftService.copyToRedshift(configuration);
        setProgress(0.85f);
        hdfsToRedshiftService.cleanupS3(configuration);
        setProgress(0.95f);
    }

    private void invokeS3Upload(HdfsToS3Configuration configuration) {
        hdfsToS3ExportService.downloadToLocal(configuration);
        setProgress(0.30f);
        hdfsToS3ExportService.upload(configuration);
        setProgress(0.99f);
    }

    private void invokeSnowflakeExport(HdfsToSnowflakeConfiguration configuration) {
        hdfsToSnowflakeService.uploadToS3(configuration);
        setProgress(0.6f);
        hdfsToSnowflakeService.copyToSnowflake(configuration);
        setProgress(0.85f);
        hdfsToSnowflakeService.cleanupS3(configuration);
        setProgress(0.95f);
    }

}
