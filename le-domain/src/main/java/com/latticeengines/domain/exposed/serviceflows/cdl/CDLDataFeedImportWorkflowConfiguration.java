package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDataFeedTaskConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.RegisterExtractConfiguration;

public class CDLDataFeedImportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public CDLDataFeedImportWorkflowConfiguration() {
    }

    public static class Builder {
        private CDLDataFeedImportWorkflowConfiguration configuration = new
                CDLDataFeedImportWorkflowConfiguration();

        private ImportDataFeedTaskConfiguration importDataFeedTaskConfiguration = new ImportDataFeedTaskConfiguration();

        private RegisterExtractConfiguration registerExtractConfiguration = new RegisterExtractConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("cdlDataFeedImportWorkflow", customerSpace, "cdlDataFeedImportWorkflow");
            importDataFeedTaskConfiguration.setCustomerSpace(customerSpace);
            registerExtractConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importDataFeedTaskConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            registerExtractConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder dataFeedTaskId(String dataFeedTaskId) {
            importDataFeedTaskConfiguration.setDataFeedTaskId(dataFeedTaskId);
            registerExtractConfiguration.setImportJobIdentifier(dataFeedTaskId);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importDataFeedTaskConfiguration.setMicroServiceHostPort(microServiceHostPort);
            registerExtractConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder importConfig(String importConfig) {
            importDataFeedTaskConfiguration.setImportConfig(importConfig);
            return this;
        }

        public CDLDataFeedImportWorkflowConfiguration build() {
            configuration.add(importDataFeedTaskConfiguration);
            configuration.add(registerExtractConfiguration);
            return configuration;
        }

    }
}
