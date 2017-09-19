package com.latticeengines.domain.exposed.serviceflows.leadprioritization;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.MatchJoinType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportStepConfiguration;

public class ImportAndRTSBulkScoreWorkflowConfiguration extends BaseLPWorkflowConfiguration {

    private ImportAndRTSBulkScoreWorkflowConfiguration() {
    }

    public static class Builder {
        private ImportAndRTSBulkScoreWorkflowConfiguration configuration = new ImportAndRTSBulkScoreWorkflowConfiguration();

        private ImportStepConfiguration importDataConfiguration = new ImportStepConfiguration();

        private BaseReportStepConfiguration registerReport = new BaseReportStepConfiguration();

        private RTSBulkScoreWorkflowConfiguration.Builder rtsBulkScoreWorkflowConfigurationBuilder = new RTSBulkScoreWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("importAndRTSBulkScoreWorkflow", customerSpace,
                    "importAndRTSBulkScoreWorkflow");
            importDataConfiguration.setCustomerSpace(customerSpace);
            registerReport.setCustomerSpace(customerSpace);
            rtsBulkScoreWorkflowConfigurationBuilder.customer(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importDataConfiguration.setMicroServiceHostPort(microServiceHostPort);
            registerReport.setMicroServiceHostPort(microServiceHostPort);
            rtsBulkScoreWorkflowConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder sourceFileName(String sourceFileName) {
            importDataConfiguration.setSourceFileName(sourceFileName);
            return this;
        }

        public Builder sourceType(SourceType sourceType) {
            importDataConfiguration.setSourceType(sourceType);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            importDataConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            registerReport.setInternalResourceHostPort(internalResourceHostPort);
            rtsBulkScoreWorkflowConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder reportNamePrefix(String reportName) {
            registerReport.setReportNamePrefix(reportName);
            return this;
        }

        public Builder inputTableName(String tableName) {
            rtsBulkScoreWorkflowConfigurationBuilder.inputTableName(tableName);
            return this;
        }

        public Builder modelId(String modelId) {
            rtsBulkScoreWorkflowConfigurationBuilder.modelId(modelId);
            return this;
        }

        public Builder outputFilename(String outputFilename) {
            rtsBulkScoreWorkflowConfigurationBuilder.outputFilename(outputFilename);
            return this;
        }

        public Builder outputFileFormat(ExportFormat format) {
            rtsBulkScoreWorkflowConfigurationBuilder.outputFileFormat(format);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder internalResourcePort(String internalResourceHostPort) {
            rtsBulkScoreWorkflowConfigurationBuilder.internalResourcePort(internalResourceHostPort);
            return this;
        }

        public Builder enableLeadEnrichment(boolean enableLeadEnrichment) {
            rtsBulkScoreWorkflowConfigurationBuilder.enableLeadEnrichment(enableLeadEnrichment);
            return this;
        }

        public Builder setScoreTestFile(boolean scoreTestFile) {
            rtsBulkScoreWorkflowConfigurationBuilder.setScoreTestFile(scoreTestFile);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            rtsBulkScoreWorkflowConfigurationBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder matchQueue(String queue) {
            rtsBulkScoreWorkflowConfigurationBuilder.matchQueue(queue);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            rtsBulkScoreWorkflowConfigurationBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder matchJoinType(MatchJoinType matchJoinType) {
            rtsBulkScoreWorkflowConfigurationBuilder.matchJoinType(matchJoinType);
            return this;
        }

        public Builder columnSelection(ColumnSelection customizedColumnSelection) {
            rtsBulkScoreWorkflowConfigurationBuilder.columnSelection(customizedColumnSelection);
            return this;
        }

        public Builder columnSelection(Predefined predefinedColumnSelection, String selectionVersion) {
            rtsBulkScoreWorkflowConfigurationBuilder.columnSelection(predefinedColumnSelection, selectionVersion);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            rtsBulkScoreWorkflowConfigurationBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder skipMatchingStep(boolean skipMatchingStep) {
            rtsBulkScoreWorkflowConfigurationBuilder.skipMatchingStep(skipMatchingStep);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            rtsBulkScoreWorkflowConfigurationBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            rtsBulkScoreWorkflowConfigurationBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public ImportAndRTSBulkScoreWorkflowConfiguration build() {

            configuration.add(importDataConfiguration);
            configuration.add(registerReport);
            configuration.add(rtsBulkScoreWorkflowConfigurationBuilder.build());
            return configuration;
        }

        public Builder enableDebug(boolean enableDebug) {
            rtsBulkScoreWorkflowConfigurationBuilder.enableDebug(enableDebug);
            return this;
        }

        public Builder matchDebugEnabled(boolean enableMatchDebug) {
            rtsBulkScoreWorkflowConfigurationBuilder.matchDebugEnabled(enableMatchDebug);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            rtsBulkScoreWorkflowConfigurationBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder modelType(String modelType) {
            rtsBulkScoreWorkflowConfigurationBuilder.modelType(modelType);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            rtsBulkScoreWorkflowConfigurationBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder bucketMetadata(List<BucketMetadata> bucketMetadata) {
            rtsBulkScoreWorkflowConfigurationBuilder.bucketMetadata(bucketMetadata);
            return this;
        }

    }
}
