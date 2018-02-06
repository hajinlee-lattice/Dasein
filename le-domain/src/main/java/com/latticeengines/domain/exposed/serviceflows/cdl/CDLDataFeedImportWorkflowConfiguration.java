package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDataFeedTaskConfiguration;

public class CDLDataFeedImportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public CDLDataFeedImportWorkflowConfiguration() {
    }

    public static class Builder {
        private CDLDataFeedImportWorkflowConfiguration configuration = new CDLDataFeedImportWorkflowConfiguration();

        private ImportDataFeedTaskConfiguration importDataFeedTaskConfiguration = new ImportDataFeedTaskConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("cdlDataFeedImportWorkflow", customerSpace,
                    "cdlDataFeedImportWorkflow");
            importDataFeedTaskConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importDataFeedTaskConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder dataFeedTaskId(String dataFeedTaskId) {
            importDataFeedTaskConfiguration.setDataFeedTaskId(dataFeedTaskId);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importDataFeedTaskConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder importConfig(String importConfig) {
            importDataFeedTaskConfiguration.setImportConfig(importConfig);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public CDLDataFeedImportWorkflowConfiguration build() {
            configuration.add(importDataFeedTaskConfiguration);
            return configuration;
        }

    }
}
