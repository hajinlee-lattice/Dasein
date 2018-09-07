package com.latticeengines.serviceflows.workflow.export;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToS3StepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component("exportScoreToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportScoreToS3 extends BaseExportToS3<ExportToS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportScoreToS3.class);

    @Override
    protected void buildRequests(List<ExportRequest> requests) {
        String outputPath = getOutputValue(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH);
        String hdfsDir = StringUtils.substringBeforeLast(outputPath, "/");
        String filePrefix = StringUtils.substringAfterLast(outputPath, "/");
        List<String> paths = null;
        try {
            paths = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir, filePrefix + ".*");
        } catch (Exception ex) {
            log.warn("Failed to get score output files. path=" + outputPath + " cause=" + ex.getMessage());
            return;
        }
        if (CollectionUtils.isNotEmpty(paths)) {
            paths.forEach(p -> {
                requests.add(new ExportRequest(p, pathBuilder.convertAtlasFile(p, podId, tenantId, s3Bucket)));
            });
        } else {
            log.warn("There was no score output files.");
        }
    }

}
