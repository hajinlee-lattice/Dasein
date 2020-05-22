package com.latticeengines.domain.exposed.serviceflows.dcp;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.DCPExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;

public class DCPSourceImportWorkflowConfiguration extends BaseDCPWorkflowConfiguration {

    public static final String WORKFLOW_NAME = "dcpSourceImportWorkflow";
    public static final String UPLOAD_ID = "UPLOAD_ID";
    public static final String SOURCE_ID = "SOURCE_ID";
    public static final String PROJECT_ID = "PROJECT_ID";

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

        public Builder uploadId(String uploadId) {
            importSourceStepConfiguration.setUploadId(uploadId);
            exportS3StepConfiguration.setUploadId(uploadId);
            return this;
        }

        public Builder statsPid(long statsPid) {
            importSourceStepConfiguration.setStatsPid(statsPid);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public DCPSourceImportWorkflowConfiguration build() {
            configuration.setContainerConfiguration(WORKFLOW_NAME, configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(importSourceStepConfiguration);
            configuration.add(exportS3StepConfiguration);
            return configuration;
        }
    }
}
