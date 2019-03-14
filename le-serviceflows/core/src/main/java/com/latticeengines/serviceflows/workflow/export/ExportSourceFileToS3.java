package com.latticeengines.serviceflows.workflow.export;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

@Component("exportSourceFileToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportSourceFileToS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(ExportSourceFileToS3.class);

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        String sourceFilePath = getStringValueFromContext(SOURCE_FILE_PATH);
        try {
            if (StringUtils.isNotBlank(sourceFilePath) &&
                    HdfsUtils.fileExists(yarnConfiguration, sourceFilePath)) {
                requests.add(new ImportExportRequest(sourceFilePath,
                        pathBuilder.convertAtlasFile(sourceFilePath, podId, tenantId, s3Bucket)));
            }
        } catch (Exception exc) {
            log.warn(String.format("Failed to copy source file=%s to S3 for tenant=%s.",
                    sourceFilePath, tenantId), exc);
        }
    }
}
