package com.latticeengines.leadprioritization.workflow;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.propdata.MatchJoinType;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.AddStandardAttributesConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.ProcessMatchResultConfiguration;
import com.latticeengines.serviceflows.workflow.scoring.ScoreStepConfiguration;

public class ScoreWorkflowConfiguration extends WorkflowConfiguration {

    private ScoreWorkflowConfiguration() {
    }

    public static class Builder {

        private ScoreWorkflowConfiguration configuration = new ScoreWorkflowConfiguration();
        private MicroserviceStepConfiguration microserviceStepConfiguration = new MicroserviceStepConfiguration();
        private MatchStepConfiguration match = new MatchStepConfiguration();
        private AddStandardAttributesConfiguration addStandardAttributes = new AddStandardAttributesConfiguration();
        private ScoreStepConfiguration score = new ScoreStepConfiguration();
        private CombineInputTableWithScoreDataFlowConfiguration combineInputWithScores = new CombineInputTableWithScoreDataFlowConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();
        private ProcessMatchResultConfiguration matchResult = new ProcessMatchResultConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("scoreWorkflow", customerSpace, "scoreWorkflow");
            microserviceStepConfiguration.setCustomerSpace(customerSpace);
            match.setCustomerSpace(customerSpace);
            addStandardAttributes.setCustomerSpace(customerSpace);
            score.setCustomerSpace(customerSpace);
            combineInputWithScores.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            matchResult.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourcePort(String internalResourceHostPort) {
            match.setInternalResourceHostPort(internalResourceHostPort);
            addStandardAttributes.setInternalResourceHostPort(internalResourceHostPort);
            score.setInternalResourceHostPort(internalResourceHostPort);
            combineInputWithScores.setInternalResourceHostPort(internalResourceHostPort);
            export.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            microserviceStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            matchResult.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            score.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder inputTableName(String tableName) {
            match.setInputTableName(tableName);
            // result table name is set during execution
            combineInputWithScores.setDataFlowParams(new CombineInputTableWithScoreParameters(null, tableName));
            return this;
        }

        public Builder modelId(String modelId) {
            score.setModelId(modelId);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            match.setDbUrl(matchClientDocument.getUrl());
            match.setDbUser(matchClientDocument.getUsername());
            match.setDbPasswordEncrypted(matchClientDocument.getEncryptedPassword());
            match.setMatchClient(matchClientDocument.getMatchClient().name());
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

        public Builder outputFilename(String outputFilename) {
            export.setExportDestination(ExportDestination.FILE);
            export.putProperty(ExportProperty.TARGET_FILE_NAME, outputFilename);
            return this;
        }

        public Builder outputFileFormat(ExportFormat format) {
            export.setExportFormat(format);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder matchJoinType(MatchJoinType matchJoinType) {
            match.setMatchJoinType(matchJoinType);
            return this;
        }

        public ScoreWorkflowConfiguration build() {
            match.microserviceStepConfiguration(microserviceStepConfiguration);
            addStandardAttributes.microserviceStepConfiguration(microserviceStepConfiguration);
            score.microserviceStepConfiguration(microserviceStepConfiguration);
            combineInputWithScores.microserviceStepConfiguration(microserviceStepConfiguration);
            export.microserviceStepConfiguration(microserviceStepConfiguration);
            match.setMatchQueue(LedpQueueAssigner.getScoringQueueNameForSubmission());

            configuration.add(match);
            configuration.add(matchResult);
            configuration.add(addStandardAttributes);
            configuration.add(score);
            configuration.add(combineInputWithScores);
            configuration.add(export);

            return configuration;
        }
    }
}
