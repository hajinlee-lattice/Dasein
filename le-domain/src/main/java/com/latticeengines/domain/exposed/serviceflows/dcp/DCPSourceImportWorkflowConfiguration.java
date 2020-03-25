package com.latticeengines.domain.exposed.serviceflows.dcp;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.DCPExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;

public class DCPSourceImportWorkflowConfiguration extends BaseDCPWorkflowConfiguration {

    public DCPSourceImportWorkflowConfiguration() {
    }

    public static class Builder {
        private DCPSourceImportWorkflowConfiguration configuration = new DCPSourceImportWorkflowConfiguration();

        private ImportSourceStepConfiguration importSourceStepConfiguration = new ImportSourceStepConfiguration();
        private DCPExportStepConfiguration exportS3StepConfiguration = new DCPExportStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            importSourceStepConfiguration.setCustomerSpace(customerSpace);
            exportS3StepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importSourceStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            exportS3StepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importSourceStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            exportS3StepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            return this;
        }

        public Builder projectId(String projectId) {
            importSourceStepConfiguration.setProjectId(projectId);
            exportS3StepConfiguration.setProjectId(projectId);
            return this;
        }

        public Builder sourceId(String sourceId) {
            importSourceStepConfiguration.setSourceId(sourceId);
            exportS3StepConfiguration.setSourceId(sourceId);
            return this;
        }

        public Builder uploadPid(Long uploadPid) {
            importSourceStepConfiguration.setUploadPid(uploadPid);
            exportS3StepConfiguration.setUploadPid(uploadPid);
            return this;
        }

        public DCPSourceImportWorkflowConfiguration build() {
            configuration.setContainerConfiguration("dcpSourceImportWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(importSourceStepConfiguration);
            configuration.add(exportS3StepConfiguration);
            return configuration;
        }
    }
}
