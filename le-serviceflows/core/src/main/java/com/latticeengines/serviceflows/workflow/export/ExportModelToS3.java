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
    protected void buildDirs(List<String> srcDirs, List<String> tgtDirs) {

        ModelSummary modelSummary = getModelSummary();
        addModelingArtifactsDirs(srcDirs, tgtDirs, modelSummary);
        addModelingSourceDirs(srcDirs, tgtDirs, modelSummary);
    }

    private void addModelingSourceDirs(List<String> srcDirs, List<String> tgtDirs, ModelSummary modelSummary) {

        addSourceFiles(srcDirs, tgtDirs, modelSummary);

        String trainingTable = modelSummary.getTrainingTableName();
        String eventTable = modelSummary.getEventTableName();
        addTableDirs(trainingTable, srcDirs, tgtDirs);
        addTableDirs(eventTable, srcDirs, tgtDirs);

    }

    private void addSourceFiles(List<String> srcDirs, List<String> tgtDirs, ModelSummary modelSummary) {
        String trainingFilePath = modelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TrainingFilePath, "");
        try {
            if (StringUtils.isNotBlank(trainingFilePath) && HdfsUtils.fileExists(yarnConfiguration, trainingFilePath)) {
                srcDirs.add(trainingFilePath);
                tgtDirs.add(pathBuilder.convertAtlasFile(trainingFilePath, podId, tenantId, s3Bucket));
            }
        } catch (Exception ex) {
            log.warn("Failed to copy file=" + trainingFilePath + " for tenantId=" + tenantId);
        }

        SourceFile sourceFile = sourceFileProxy.findByTableName(customer, modelSummary.getTrainingTableName());
        try {
            if (sourceFile != null && StringUtils.isNotBlank(sourceFile.getPath())
                    && !sourceFile.getPath().equals(trainingFilePath)
                    && HdfsUtils.fileExists(yarnConfiguration, sourceFile.getPath())) {
                srcDirs.add(sourceFile.getPath());
                tgtDirs.add(pathBuilder.convertAtlasFile(sourceFile.getPath(), podId, tenantId, s3Bucket));
            }
        } catch (Exception ex) {
            log.warn("Failed to copy file=" + sourceFile.getPath() + " for tenantId=" + tenantId);
        }
    }

    private void addTableDirs(String tableName, List<String> srcDirs, List<String> tgtDirs) {
        if (StringUtils.isNotBlank(tableName)) {
            Table table = metadataProxy.getTable(customer, tableName);
            if (table != null) {
                List<Extract> extracts = table.getExtracts();
                if (CollectionUtils.isNotEmpty(extracts)) {
                    extracts.forEach(extract -> {
                        if (StringUtils.isNotBlank(extract.getPath())) {
                            String srcDir = pathBuilder.getFullPath(extract.getPath());
                            String tgtDir = pathBuilder.convertAtlasTableDir(srcDir, podId, tenantId, s3Bucket);
                            srcDirs.add(srcDir);
                            tgtDirs.add(tgtDir);
                        }
                    });
                }
            } else {
                log.warn("Can not find the table=" + tableName + " for tenant=" + customer);
            }
        }
    }

    private void addModelingArtifactsDirs(List<String> srcDirs, List<String> tgtDirs, ModelSummary modelSummary) {
        String[] parts = modelSummary.getLookupId().split("\\|");
        String eventTable = parts[1];
        srcDirs.add(pathBuilder.getHdfsAnalyticsModelTableDir(customer, eventTable));
        srcDirs.add(pathBuilder.getHdfsAnalyticsDataTableDir(customer, eventTable));
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
        srcDirs.add(hdfsMetadataDir);

        tgtDirs.add(pathBuilder.getS3AnalyticsModelTableDir(s3Bucket, tenantId, eventTable));
        tgtDirs.add(pathBuilder.getS3AnalyticsDataTableDir(s3Bucket, tenantId, eventTable));
        String s3MetadataDir = isEvent
                ? pathBuilder.getS3AnalyticsMetaDataTableDir(s3Bucket, tenantId, eventTable, "Event")
                : pathBuilder.getS3AnalyticsMetaDataTableDir(s3Bucket, tenantId, eventTable, "Target");
        tgtDirs.add(s3MetadataDir);
    }

    private ModelSummary getModelSummary() {
        String modelId = getStringValueFromContext(SCORING_MODEL_ID);
        return modelSummaryProxy.getModelSummaryByModelId(getConfiguration().getCustomerSpace().toString(), modelId);
    }

}
