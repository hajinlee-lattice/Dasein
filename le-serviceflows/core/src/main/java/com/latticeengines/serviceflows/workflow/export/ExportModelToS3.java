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
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("exportModelToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportModelToS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportModelToS3.class);

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {

        ModelSummary modelSummary = getModelSummary();
        addModelingArtifactsDirs(requests, modelSummary);
        addModelingSourceDirs(requests, modelSummary);
        addScoringTrainingFile(requests);
    }

    private void addScoringTrainingFile(List<ImportExportRequest> requests) {
        String tableName = getStringValueFromContext(EXPORT_SCORE_TRAINING_FILE_TABLE_NAME);
        if (StringUtils.isNotBlank(tableName)) {
            addTableDirs(tableName, requests);
        }
    }

    private void addModelingSourceDirs(List<ImportExportRequest> requests, ModelSummary modelSummary) {

        addSourceFiles(requests, modelSummary);

        String trainingTable = modelSummary.getTrainingTableName();
        String eventTable = modelSummary.getEventTableName();
        addTableDirs(trainingTable, requests);
        addTableDirs(eventTable, requests);

    }

    protected void addSourceFiles(List<ImportExportRequest> requests, ModelSummary modelSummary) {
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

    private void addSourceFile(List<ImportExportRequest> requests, ModelSummary modelSummary, String trainingFilePath) {
        if (StringUtils.isBlank(modelSummary.getTrainingTableName())) {
            return;
        }
        try {
            SourceFile sourceFile = sourceFileProxy.findByTableName(customer, modelSummary.getTrainingTableName());
            if (sourceFile != null && StringUtils.isNotBlank(sourceFile.getPath())
                    && !sourceFile.getPath().equals(trainingFilePath)
                    && HdfsUtils.fileExists(yarnConfiguration, sourceFile.getPath())) {
                requests.add(new ImportExportRequest(sourceFile.getPath(),
                        pathBuilder.convertAtlasFile(sourceFile.getPath(), podId, tenantId, s3Bucket)));
            }
        } catch (Exception ex) {
            log.warn("Failed to copy file for training table=" + modelSummary.getTrainingTableName() + " for tenantId="
                    + tenantId);
        }
    }

    protected void addAtlasMetadataFile(List<ImportExportRequest> requests, String filePath) {
        try {
            if (StringUtils.isNotBlank(filePath) && HdfsUtils.fileExists(yarnConfiguration, filePath)) {
                requests.add(new ImportExportRequest(filePath,
                        pathBuilder.convertAtlasMetadata(filePath, podId, tenantId, s3Bucket)));
            }
        } catch (Exception ex) {
            log.warn("Failed to copy file=" + filePath + " for tenantId=" + tenantId, " cause=" + ex.getMessage());
        }
    }

    protected void addAtlasFile(List<ImportExportRequest> requests, String filePath) {
        try {
            if (StringUtils.isNotBlank(filePath) && HdfsUtils.fileExists(yarnConfiguration, filePath)) {
                requests.add(new ImportExportRequest(filePath,
                        pathBuilder.convertAtlasFile(filePath, podId, tenantId, s3Bucket)));
            }
        } catch (Exception ex) {
            log.warn("Failed to copy file=" + filePath + " for tenantId=" + tenantId, " cause=" + ex.getMessage());
        }
    }

    private void addTableDirs(String tableName, List<ImportExportRequest> requests) {
        if (StringUtils.isNotBlank(tableName)) {
            Table table = metadataProxy.getTable(customer, tableName);
            if (table != null) {
                List<Extract> extracts = table.getExtracts();
                if (CollectionUtils.isNotEmpty(extracts)) {
                    extracts.forEach(extract -> {
                        if (StringUtils.isNotBlank(extract.getPath())) {
                            String srcDir = pathBuilder.getFullPath(extract.getPath());
                            String tgtDir = pathBuilder.convertAtlasTableDir(srcDir, podId, tenantId, s3Bucket);
                            requests.add(new ImportExportRequest(srcDir, tgtDir, tableName, true, true));
                        }
                    });
                }
            } else {
                log.warn("Can not find the table=" + tableName + " for tenant=" + customer);
            }
        }
    }

    private void addModelingArtifactsDirs(List<ImportExportRequest> requests, ModelSummary modelSummary) {
        String[] parts = modelSummary.getLookupId().split("\\|");
        String eventTableName = parts[1];

        ImportExportRequest request = new ImportExportRequest();
        request.srcPath = pathBuilder.getHdfsAnalyticsModelTableDir(customer, eventTableName);
        request.tgtPath = pathBuilder.getS3AnalyticsModelTableDir(s3Bucket, tenantId, eventTableName);
        requests.add(request);

        request = new ImportExportRequest();
        request.srcPath = pathBuilder.getHdfsAnalyticsDataTableDir(customer, eventTableName);
        request.tgtPath = pathBuilder.getS3AnalyticsDataTableDir(s3Bucket, tenantId, eventTableName);
        requests.add(request);

        request = new ImportExportRequest();
        String eventColumn = "Event";
        Table eventTable = metadataProxy.getTable(customer, eventTableName);
        if (eventTable == null) {
            log.warn("There was no event table.");
        } else {
            List<Attribute> events = eventTable.getAttributes(LogicalDataType.Event);
            if (CollectionUtils.isNotEmpty(events)) {
                eventColumn = events.get(0).getDisplayName();
            }
        }
        request.srcPath = pathBuilder.getHdfsAnalyticsMetaDataTableDir(customer, eventTableName, eventColumn);
        request.tgtPath = pathBuilder.getS3AnalyticsMetaDataTableDir(s3Bucket, tenantId, eventTableName, eventColumn);
        requests.add(request);
    }

    private ModelSummary getModelSummary() {
        String modelId = getStringValueFromContext(SCORING_MODEL_ID);
        return modelSummaryProxy.getByModelId(modelId);
    }

}
