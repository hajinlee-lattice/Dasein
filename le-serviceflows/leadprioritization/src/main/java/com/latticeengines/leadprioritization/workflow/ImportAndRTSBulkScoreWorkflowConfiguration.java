package com.latticeengines.leadprioritization.workflow;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.importdata.ImportStepConfiguration;
import com.latticeengines.serviceflows.workflow.report.BaseReportStepConfiguration;

public class ImportAndRTSBulkScoreWorkflowConfiguration extends WorkflowConfiguration {

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

        public ImportAndRTSBulkScoreWorkflowConfiguration build() {
            configuration.add(importDataConfiguration);
            configuration.add(registerReport);
            configuration.add(rtsBulkScoreWorkflowConfigurationBuilder.build());
            return configuration;
        }

    }
}
