package com.latticeengines.eai.yarn.runtime;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.route.HdfsToS3Configuration;
import com.latticeengines.domain.exposed.eai.route.HdfsToSnowflakeConfiguration;
import com.latticeengines.eai.service.impl.s3.HdfsToS3ExportService;
import com.latticeengines.eai.service.impl.snowflake.HdfsToSnowflakeService;

@Component("exportProcessor")
public class ExportProcessor extends SingleContainerYarnProcessor<ExportConfiguration>
        implements ItemProcessor<ExportConfiguration, String> {

    @Autowired
    private HdfsToS3ExportService hdfsToS3ExportService;

    @Autowired
    private HdfsToSnowflakeService hdfsToSnowflakeService;

    @Override
    public String process(ExportConfiguration exportConfig) throws Exception {
        if (exportConfig instanceof HdfsToS3Configuration) {
            invokeS3Upload((HdfsToS3Configuration) exportConfig);
        } else if (exportConfig instanceof HdfsToSnowflakeConfiguration) {
            invokeSnowflakeExport((HdfsToSnowflakeConfiguration) exportConfig);
        }
        return null;
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
