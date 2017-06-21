package com.latticeengines.domain.exposed.serviceflows.leadprioritization;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.MatchJoinType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ProcessMatchResultConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RTSScoreStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CombineMatchDebugWithScoreDataFlowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class RTSBulkScoreWorkflowConfiguration extends BaseLPWorkflowConfiguration {

    private RTSBulkScoreWorkflowConfiguration() {
    }

    public static class Builder {

        private RTSBulkScoreWorkflowConfiguration configuration = new RTSBulkScoreWorkflowConfiguration();
        private MicroserviceStepConfiguration microserviceStepConfiguration = new MicroserviceStepConfiguration();

        private RTSScoreStepConfiguration score = new RTSScoreStepConfiguration();
        private CombineInputTableWithScoreDataFlowConfiguration combineInputWithScores = new CombineInputTableWithScoreDataFlowConfiguration();
        private CombineMatchDebugWithScoreDataFlowConfiguration combineMatchDebugWithScores = new CombineMatchDebugWithScoreDataFlowConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();
        private MatchStepConfiguration match = new MatchStepConfiguration();
        private ProcessMatchResultConfiguration matchResult = new ProcessMatchResultConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("rtsBulkScoreWorkflow", customerSpace, "rtsBulkScoreWorkflow");
            microserviceStepConfiguration.setCustomerSpace(customerSpace);

            match.setCustomerSpace(customerSpace);
            matchResult.setCustomerSpace(customerSpace);
            score.setCustomerSpace(customerSpace);
            combineInputWithScores.setCustomerSpace(customerSpace);
            combineMatchDebugWithScores.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourcePort(String internalResourceHostPort) {

            match.setInternalResourceHostPort(internalResourceHostPort);
            score.setInternalResourceHostPort(internalResourceHostPort);
            combineInputWithScores.setInternalResourceHostPort(internalResourceHostPort);
            combineMatchDebugWithScores.setInternalResourceHostPort(internalResourceHostPort);
            export.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            microserviceStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            matchResult.setMicroServiceHostPort(microServiceHostPort);

            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            score.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            match.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder inputTableName(String tableName) {
            match.setInputTableName(tableName);
            score.setInputTableName(tableName);
            // result table name is set during execution
            combineInputWithScores.setDataFlowParams(new CombineInputTableWithScoreParameters(null, tableName));
            return this;
        }

        public Builder modelId(String modelId) {
            score.setModelId(modelId);
            return this;
        }

        public Builder outputFilename(String outputFilename) {
            export.setExportDestination(ExportDestination.FILE);
            export.putProperty(ExportProperty.TARGET_FILE_NAME, outputFilename);
            return this;
        }

        public Builder outputFileFormat(ExportFormat format) {
            export.setExportFormat(format);
            return this;
        }

        public Builder enableLeadEnrichment(boolean enableLeadEnrichment) {
            score.setEnableLeadEnrichment(enableLeadEnrichment);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            match.setMatchCommandType(matchCommandType);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            match.setDestTables(destTables);
            return this;
        }

        public Builder matchJoinType(MatchJoinType matchJoinType) {
            match.setMatchJoinType(matchJoinType);
            return this;
        }

        public Builder columnSelection(ColumnSelection customizedColumnSelection) {
            match.setCustomizedColumnSelection(customizedColumnSelection);
            return this;
        }

        public Builder columnSelection(Predefined predefinedColumnSelection, String selectionVersion) {
            match.setPredefinedColumnSelection(predefinedColumnSelection);
            match.setPredefinedSelectionVersion(selectionVersion);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            matchResult.setExcludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder skipMatchingStep(boolean skipMatchingStep) {
            match.setSkipStep(skipMatchingStep);
            matchResult.setSkipStep(skipMatchingStep);
            return this;
        }

        public Builder matchDebugEnabled(boolean matchDebugEnabled) {
            combineMatchDebugWithScores.setSkipStep(!matchDebugEnabled);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            match.setMatchRequestSource(matchRequestSource);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            match.setDataCloudVersion(dataCloudVersion);
            matchResult.setDataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            match.setDbUrl(matchClientDocument.getUrl());
            match.setDbUser(matchClientDocument.getUsername());
            match.setDbPasswordEncrypted(matchClientDocument.getEncryptedPassword());
            match.setMatchClient(matchClientDocument.getMatchClient().name());
            return this;
        }

        public Builder matchQueue(String queue) {
            match.setMatchQueue(queue);
            return this;
        }

        public RTSBulkScoreWorkflowConfiguration build() {
            match.microserviceStepConfiguration(microserviceStepConfiguration);
            score.microserviceStepConfiguration(microserviceStepConfiguration);
            combineInputWithScores.microserviceStepConfiguration(microserviceStepConfiguration);
            combineMatchDebugWithScores.microserviceStepConfiguration(microserviceStepConfiguration);
            export.microserviceStepConfiguration(microserviceStepConfiguration);

            configuration.add(match);
            configuration.add(matchResult);
            configuration.add(score);
            configuration.add(combineInputWithScores);
            configuration.add(combineMatchDebugWithScores);
            configuration.add(export);

            return configuration;
        }

        public Builder enableDebug(boolean enableDebug) {
            score.setEnableDebug(enableDebug);
            return this;
        }

        public Builder modelType(String modelType) {
            score.setModelType(modelType);
            combineInputWithScores.setModelType(modelType);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            match.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder bucketMetadata(List<BucketMetadata> bucketMetadataList) {
            combineInputWithScores.setBucketMetadata(bucketMetadataList);
            return this;
        }
    }
}
