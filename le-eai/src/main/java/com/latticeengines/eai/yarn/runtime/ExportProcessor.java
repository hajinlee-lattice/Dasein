package com.latticeengines.eai.yarn.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.eai.HdfsToS3Configuration;
import com.latticeengines.eai.service.impl.redshift.HdfsToRedshiftService;
import com.latticeengines.eai.service.impl.s3.HdfsToS3ExportService;

@Component("exportProcessor")
public class ExportProcessor extends SingleContainerYarnProcessor<ExportConfiguration>
        implements ItemProcessor<ExportConfiguration, String> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ExportProcessor.class);

    @Autowired
    private HdfsToS3ExportService hdfsToS3ExportService;

    @Autowired
    private HdfsToRedshiftService hdfsToRedshiftService;

    @Override
    public String process(ExportConfiguration exportConfig) throws Exception {
        if (exportConfig instanceof HdfsToS3Configuration) {
            invokeS3Upload((HdfsToS3Configuration) exportConfig);
        } else if (exportConfig instanceof HdfsToRedshiftConfiguration) {
            invokeRedshiftExport((HdfsToRedshiftConfiguration) exportConfig);
        }
        return null;
    }

    private void invokeRedshiftExport(HdfsToRedshiftConfiguration configuration) {
        if (!configuration.isSkipS3Upload()) {
            hdfsToRedshiftService.cleanupS3(configuration);
            setProgress(0.1f);
            hdfsToRedshiftService.uploadJsonPathSchema(configuration);
            setProgress(0.2f);
            hdfsToRedshiftService.uploadDataObjectToS3(configuration);
            setProgress(0.6f);
        }
        if (configuration.isCreateNew()) {
            hdfsToRedshiftService.dropRedshiftTable(configuration);
        }
        hdfsToRedshiftService.createRedshiftTableIfNotExist(configuration);
        setProgress(0.65f);
        if (configuration.isAppend()) {
            hdfsToRedshiftService.copyToRedshift(configuration);
        } else {
            hdfsToRedshiftService.updateExistingRows(configuration);
        }
        setProgress(0.95f);
    }

    private void invokeS3Upload(HdfsToS3Configuration configuration) {
        hdfsToS3ExportService.downloadToLocal(configuration);
        setProgress(0.30f);
        hdfsToS3ExportService.upload(configuration);
        setProgress(0.99f);
    }

}
