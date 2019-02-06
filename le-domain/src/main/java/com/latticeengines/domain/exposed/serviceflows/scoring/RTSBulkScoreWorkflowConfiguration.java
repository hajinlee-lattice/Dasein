package com.latticeengines.domain.exposed.serviceflows.scoring;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.MatchJoinType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CombineMatchDebugWithScoreDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ExportScoreTrainingFileStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RTSScoreStepConfiguration;

public class RTSBulkScoreWorkflowConfiguration extends BaseScoringWorkflowConfiguration {

    private RTSBulkScoreWorkflowConfiguration() {
    }

    public static class Builder {

        private RTSBulkScoreWorkflowConfiguration configuration = new RTSBulkScoreWorkflowConfiguration();
        private MicroserviceStepConfiguration microserviceStepConfiguration = new MicroserviceStepConfiguration();

        private RTSScoreStepConfiguration score = new RTSScoreStepConfiguration();
        private CombineInputTableWithScoreDataFlowConfiguration combineInputWithScores = new CombineInputTableWithScoreDataFlowConfiguration();
        private CombineMatchDebugWithScoreDataFlowConfiguration combineMatchDebugWithScores = new CombineMatchDebugWithScoreDataFlowConfiguration();
        private ExportScoreTrainingFileStepConfiguration export = new ExportScoreTrainingFileStepConfiguration();
        private MatchDataCloudWorkflowConfiguration.Builder matchDataCloudWorkflowBuilder = new MatchDataCloudWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            microserviceStepConfiguration.setCustomerSpace(customerSpace);
            matchDataCloudWorkflowBuilder.customer(customerSpace);
            score.setCustomerSpace(customerSpace);
            combineInputWithScores.setCustomerSpace(customerSpace);
            combineMatchDebugWithScores.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            microserviceStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            matchDataCloudWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            matchDataCloudWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            score.setInternalResourceHostPort(internalResourceHostPort);
            combineInputWithScores.setInternalResourceHostPort(internalResourceHostPort);
            combineMatchDebugWithScores.setInternalResourceHostPort(internalResourceHostPort);
            export.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder inputTableName(String tableName) {
            matchDataCloudWorkflowBuilder.matchInputTableName(tableName);
            // result table name is set during execution
            score.setInputTableName(tableName);
            combineInputWithScores
                    .setDataFlowParams(new CombineInputTableWithScoreParameters(null, tableName));
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
            matchDataCloudWorkflowBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            matchDataCloudWorkflowBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder matchJoinType(MatchJoinType matchJoinType) {
            matchDataCloudWorkflowBuilder.matchJoinType(matchJoinType);
            return this;
        }

        public Builder matchColumnSelection(Predefined predefinedColumnSelection,
                String selectionVersion) {
            matchDataCloudWorkflowBuilder.matchColumnSelection(predefinedColumnSelection,
                    selectionVersion);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            matchDataCloudWorkflowBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder skipBulkMatch(boolean skipMatchingStep) {
            matchDataCloudWorkflowBuilder.skipMatchingStep(skipMatchingStep);
            return this;
        }

        public Builder skipMatching(boolean skipMatchingStep) {
            skipBulkMatch(skipMatchingStep);
            score.setEnableMatching(!skipMatchingStep);
            return this;
        }

        public Builder matchDebugEnabled(boolean matchDebugEnabled) {
            combineMatchDebugWithScores.setSkipStep(!matchDebugEnabled);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            matchDataCloudWorkflowBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            matchDataCloudWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            matchDataCloudWorkflowBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder matchQueue(String queue) {
            matchDataCloudWorkflowBuilder.matchQueue(queue);
            return this;
        }

        public Builder workflowContainerMem(int mb) {
            configuration.setContainerMemoryMB(mb);
            return this;
        }

        public Builder fetchOnly(boolean fetchOnly) {
            matchDataCloudWorkflowBuilder.fetchOnly(fetchOnly);
            return this;
        }

        public RTSBulkScoreWorkflowConfiguration build() {
            configuration.setContainerConfiguration("rtsBulkScoreWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            score.microserviceStepConfiguration(microserviceStepConfiguration);
            combineInputWithScores.microserviceStepConfiguration(microserviceStepConfiguration);
            combineMatchDebugWithScores
                    .microserviceStepConfiguration(microserviceStepConfiguration);
            export.microserviceStepConfiguration(microserviceStepConfiguration);

            configuration.add(matchDataCloudWorkflowBuilder.build());
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

        public Builder setScoreTestFile(boolean scoreTestFile) {
            score.setScoreTestFile(scoreTestFile);
            return this;
        }

        public Builder modelType(String modelType) {
            score.setModelType(modelType);
            combineInputWithScores.setModelType(modelType);
            return this;
        }

        public Builder idColumnName(String idColumnName) {
            score.setIdColumnName(idColumnName);
            combineInputWithScores.setIdColumnName(idColumnName);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            matchDataCloudWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder bucketMetadata(List<BucketMetadata> bucketMetadataList) {
            combineInputWithScores.setBucketMetadata(bucketMetadataList);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            matchDataCloudWorkflowBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder treatPublicDomainAsNormalDomain(boolean publicDomainAsNormalDomain) {
            matchDataCloudWorkflowBuilder
                    .treatPublicDomainAsNormalDomain(publicDomainAsNormalDomain);
            return this;
        }

        public Builder excludePublicDomain(boolean excludePublicDomains) {
            matchDataCloudWorkflowBuilder.excludePublicDomains(excludePublicDomains);
            return this;
        }

    }
}
