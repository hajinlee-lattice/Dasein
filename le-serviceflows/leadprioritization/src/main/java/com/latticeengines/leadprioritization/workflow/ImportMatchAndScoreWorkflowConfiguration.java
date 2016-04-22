package com.latticeengines.leadprioritization.workflow;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.propdata.MatchJoinType;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.importdata.ImportStepConfiguration;
import com.latticeengines.serviceflows.workflow.report.BaseReportStepConfiguration;

public class ImportMatchAndScoreWorkflowConfiguration extends WorkflowConfiguration {

    private ImportMatchAndScoreWorkflowConfiguration() {
    }

    public static class Builder {
        private ImportMatchAndScoreWorkflowConfiguration configuration = new ImportMatchAndScoreWorkflowConfiguration();

        private ImportStepConfiguration importDataConfiguration = new ImportStepConfiguration();

        private BaseReportStepConfiguration registerReport = new BaseReportStepConfiguration();

        private ScoreWorkflowConfiguration.Builder scoreWorkflowConfigurationBuilder = new ScoreWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("importMatchAndScoreWorkflow", customerSpace,
                    "importMatchAndScoreWorkflow");
            importDataConfiguration.setCustomerSpace(customerSpace);
            registerReport.setCustomerSpace(customerSpace);
            scoreWorkflowConfigurationBuilder.customer(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importDataConfiguration.setMicroServiceHostPort(microServiceHostPort);
            registerReport.setMicroServiceHostPort(microServiceHostPort);
            scoreWorkflowConfigurationBuilder.microServiceHostPort(microServiceHostPort);
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
            scoreWorkflowConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder reportName(String reportName) {
            registerReport.setReportName(reportName);
            return this;
        }

        public Builder inputTableName(String tableName) {
            scoreWorkflowConfigurationBuilder.inputTableName(tableName);
            return this;
        }

        public Builder modelId(String modelId) {
            scoreWorkflowConfigurationBuilder.modelId(modelId);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            scoreWorkflowConfigurationBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            scoreWorkflowConfigurationBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            scoreWorkflowConfigurationBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder outputFilename(String outputFilename) {
            scoreWorkflowConfigurationBuilder.outputFilename(outputFilename);
            return this;
        }

        public Builder outputFileFormat(ExportFormat format) {
            scoreWorkflowConfigurationBuilder.outputFileFormat(format);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder internalResourcePort(String internalResourceHostPort) {
            scoreWorkflowConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder matchJoinType(MatchJoinType matchJoinType) {
            scoreWorkflowConfigurationBuilder.matchJoinType(matchJoinType);
            return this;
        }

        public ImportMatchAndScoreWorkflowConfiguration build() {
            configuration.add(importDataConfiguration);
            configuration.add(registerReport);
            configuration.add(scoreWorkflowConfigurationBuilder.build());
            return configuration;
        }

    }
}
