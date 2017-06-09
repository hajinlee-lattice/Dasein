package com.latticeengines.cdl.workflow;

import com.latticeengines.cdl.workflow.steps.importdata.ImportDataFeedTaskConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class CDLDataFeedImportWorkflowConfiguration extends WorkflowConfiguration {

//    private String microServiceHostPort;

    public CDLDataFeedImportWorkflowConfiguration() {
    }

//    public String getMicroServiceHostPort() {
//        return microServiceHostPort;
//    }
//
//    public void setMicroServiceHostPort(String microServiceHostPort) {
//        this.microServiceHostPort = microServiceHostPort;
//    }

    public static class Builder {
        private CDLDataFeedImportWorkflowConfiguration configuration = new
                CDLDataFeedImportWorkflowConfiguration();

        private ImportDataFeedTaskConfiguration importDataFeedTaskConfiguration = new ImportDataFeedTaskConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("cdlDataFeedImportWorkflow", customerSpace, "cdlDataFeedImportWorkflow");
            importDataFeedTaskConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

//        public Builder microServiceHostPort(String microServiceHostPort) {
//            configuration.setMicroServiceHostPort(microServiceHostPort);
//            return this;
//        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importDataFeedTaskConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder dataFeedTaskId(Long dataFeedTaskId) {
            importDataFeedTaskConfiguration.setDataFeedTaskId(dataFeedTaskId);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importDataFeedTaskConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

//        public Builder stagingDir(String stagingDir) {
//            importDataFeedTaskConfiguration.setStagingDir(stagingDir);
//            return this;
//        }

        public Builder importConfig(String importConfig) {
            importDataFeedTaskConfiguration.setImportConfig(importConfig);
            return this;
        }

        public CDLDataFeedImportWorkflowConfiguration build() {
            configuration.add(importDataFeedTaskConfiguration);
            return configuration;
        }

    }
}
