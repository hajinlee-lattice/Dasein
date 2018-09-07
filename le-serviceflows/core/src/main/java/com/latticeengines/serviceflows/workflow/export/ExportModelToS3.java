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
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToS3StepConfiguration;

@Component("exportModelToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportModelToS3 extends BaseExportToS3<ExportToS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportModelToS3.class);

    @Override
    protected void buildRequests(List<ExportRequest> requests) {

        ModelSummary modelSummary = getModelSummary();
        addModelingArtifactsDirs(requests, modelSummary);
        addModelingSourceDirs(requests, modelSummary);
    }

    private void addModelingSourceDirs(List<ExportRequest> requests, ModelSummary modelSummary) {

        addSourceFiles(requests, modelSummary);

        String trainingTable = modelSummary.getTrainingTableName();
        String eventTable = modelSummary.getEventTableName();
        addTableDirs(trainingTable, requests);
        addTableDirs(eventTable, requests);

    }

    protected void addSourceFiles(List<ExportRequest> requests, ModelSummary modelSummary) {
        String trainingFilePath = modelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TrainingFilePath, "");
        addAtlasFile(requests, trainingFilePath);
        addSourceFile(requests, modelSummary, trainingFilePath);

        String pmmlFilePath = modelSummary.getModelSummaryConfiguration().getString(ProvenancePropertyName.PmmlFilePath,
                "");
        addAtlasMetadataFile(requests, pmmlFilePath);
        if (StringUtils.isNotBlank(pmmlFilePath) && pmmlFilePath.endsWith(".fixed.xml")) {
            String pmmlOrigFilePath = StringUtils.substringBeforeLast(pmmlFilePath, ".fixed.xml");
            addAtlasMetadataFile(requests, pmmlOrigFilePath);
        } else if (StringUtils.isNotBlank(pmmlFilePath)) {
            String pmmlFixedFilePath = pmmlFilePath + ".fixed.xml";
            addAtlasMetadataFile(requests, pmmlFixedFilePath);
        }
        String pivotFilePath = modelSummary.getPivotArtifactPath();
        addAtlasMetadataFile(requests, pivotFilePath);
    }

    private void addSourceFile(List<ExportRequest> requests, ModelSummary modelSummary, String trainingFilePath) {
        if (StringUtils.isBlank(modelSummary.getTrainingTableName())) {
            return;
        }
        try {
            SourceFile sourceFile = sourceFileProxy.findByTableName(customer, modelSummary.getTrainingTableName());
            if (sourceFile != null && StringUtils.isNotBlank(sourceFile.getPath())
                    && !sourceFile.getPath().equals(trainingFilePath)
                    && HdfsUtils.fileExists(yarnConfiguration, sourceFile.getPath())) {
                requests.add(new ExportRequest(sourceFile.getPath(),
                        pathBuilder.convertAtlasFile(sourceFile.getPath(), podId, tenantId, s3Bucket)));
            }
        } catch (Exception ex) {
            log.warn("Failed to copy file for taining table =" + modelSummary.getTrainingTableName() + " for tenantId="
                    + tenantId);
        }
    }

    protected void addAtlasMetadataFile(List<ExportRequest> requests, String filePath) {
        try {
            if (StringUtils.isNotBlank(filePath) && HdfsUtils.fileExists(yarnConfiguration, filePath)) {
                requests.add(new ExportRequest(filePath,
                        pathBuilder.convertAtlasMetadata(filePath, podId, tenantId, s3Bucket)));
            }
        } catch (Exception ex) {
            log.warn("Failed to copy file=" + filePath + " for tenantId=" + tenantId, " cause=" + ex.getMessage());
        }
    }

    protected void addAtlasFile(List<ExportRequest> requests, String filePath) {
        try {
            if (StringUtils.isNotBlank(filePath) && HdfsUtils.fileExists(yarnConfiguration, filePath)) {
                requests.add(
                        new ExportRequest(filePath, pathBuilder.convertAtlasFile(filePath, podId, tenantId, s3Bucket)));
            }
        } catch (Exception ex) {
            log.warn("Failed to copy file=" + filePath + " for tenantId=" + tenantId, " cause=" + ex.getMessage());
        }
    }

    private void addTableDirs(String tableName, List<ExportRequest> requests) {
        if (StringUtils.isNotBlank(tableName)) {
            Table table = metadataProxy.getTable(customer, tableName);
            if (table != null) {
                List<Extract> extracts = table.getExtracts();
                if (CollectionUtils.isNotEmpty(extracts)) {
                    extracts.forEach(extract -> {
                        if (StringUtils.isNotBlank(extract.getPath())) {
                            String srcDir = pathBuilder.getFullPath(extract.getPath());
                            String tgtDir = pathBuilder.convertAtlasTableDir(srcDir, podId, tenantId, s3Bucket);
                            requests.add(new ExportRequest(srcDir, tgtDir));
                        }
                    });
                }
            } else {
                log.warn("Can not find the table=" + tableName + " for tenant=" + customer);
            }
        }
    }

    private void addModelingArtifactsDirs(List<ExportRequest> requests, ModelSummary modelSummary) {
        String[] parts = modelSummary.getLookupId().split("\\|");
        String eventTable = parts[1];

        ExportRequest request = new ExportRequest();
        request.srcDir = pathBuilder.getHdfsAnalyticsModelTableDir(customer, eventTable);
        request.tgtDir = pathBuilder.getS3AnalyticsModelTableDir(s3Bucket, tenantId, eventTable);
        requests.add(request);

        request = new ExportRequest();
        request.srcDir = pathBuilder.getHdfsAnalyticsDataTableDir(customer, eventTable);
        request.tgtDir = pathBuilder.getS3AnalyticsDataTableDir(s3Bucket, tenantId, eventTable);
        requests.add(request);

        String hdfsMetadataDir = pathBuilder.getHdfsAnalyticsMetaDataTableDir(customer, eventTable, "Event");
        boolean isEvent = true;
        try {
            if (!HdfsUtils.fileExists(yarnConfiguration, hdfsMetadataDir)) {
                hdfsMetadataDir = pathBuilder.getHdfsAnalyticsMetaDataTableDir(customer, eventTable, "Target");
                isEvent = false;
            }
        } catch (Exception ex) {
            log.warn("Failed to get modeling Metadata dir!");
            return;
        }
        request = new ExportRequest();
        request.srcDir = hdfsMetadataDir;
        String s3MetadataDir = isEvent
                ? pathBuilder.getS3AnalyticsMetaDataTableDir(s3Bucket, tenantId, eventTable, "Event")
                : pathBuilder.getS3AnalyticsMetaDataTableDir(s3Bucket, tenantId, eventTable, "Target");
        request.tgtDir = s3MetadataDir;
        requests.add(request);
    }

    private ModelSummary getModelSummary() {
        String modelId = getStringValueFromContext(SCORING_MODEL_ID);
        return modelSummaryProxy.getModelSummaryByModelId(getConfiguration().getCustomerSpace().toString(), modelId);
    }

}
