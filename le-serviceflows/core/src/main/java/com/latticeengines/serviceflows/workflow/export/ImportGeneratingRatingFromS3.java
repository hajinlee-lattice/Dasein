package com.latticeengines.serviceflows.workflow.export;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("importGeneratingRatingFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportGeneratingRatingFromS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ImportGeneratingRatingFromS3.class);

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {

        String modelIdStr = getStringValueFromContext(SCORING_MODEL_ID);
        if (StringUtils.isBlank(modelIdStr)) {
            return;
        }
        String[] modelIds = modelIdStr.split("\\|");
        log.info("Get model ids: " + StringUtils.join(modelIds, ", "));
        for (String modelId : modelIds) {
            addModelingArtifactsDirs(modelId, requests);
        }

    }

    private void addModelingArtifactsDirs(String modelId, List<ImportExportRequest> requests) {
        ModelSummary modelSummary = modelSummaryProxy.getByModelId(modelId);
        String[] parts = modelSummary.getLookupId().split("\\|");
        String eventTableName = parts[1];
        ImportExportRequest request = new ImportExportRequest();
        request.tgtPath = pathBuilder.getHdfsAnalyticsModelTableDir(customer, eventTableName);
        request.srcPath = pathBuilder.getS3AnalyticsModelTableDir(s3Bucket, tenantId, eventTableName);
        request.tableName = eventTableName;
        request.isSync = true;
        try {
            if (!HdfsUtils.fileExists(distCpConfiguration, request.tgtPath)) {
                requests.add(request);
                return;
            }
        } catch (Exception ex) {
            log.warn("Failed to add ModelData, error=" + ex.getMessage());
            return;
        }
    }

}
