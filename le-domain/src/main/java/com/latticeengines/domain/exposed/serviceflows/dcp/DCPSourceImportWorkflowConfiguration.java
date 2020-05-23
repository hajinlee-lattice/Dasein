package com.latticeengines.domain.exposed.serviceflows.dcp;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchConfig;
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

        private ImportSourceStepConfiguration importSource = new ImportSourceStepConfiguration();
        private DCPExportStepConfiguration exportS3 = new DCPExportStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            importSource.setCustomerSpace(customerSpace);
            exportS3.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importSource.setInternalResourceHostPort(internalResourceHostPort);
            exportS3.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importSource.setMicroServiceHostPort(microServiceHostPort);
            exportS3.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            return this;
        }

        public Builder projectId(String projectId) {
            importSource.setProjectId(projectId);
            exportS3.setProjectId(projectId);
            return this;
        }

        public Builder sourceId(String sourceId) {
            importSource.setSourceId(sourceId);
            exportS3.setSourceId(sourceId);
            return this;
        }

        public Builder uploadId(String uploadId) {
            importSource.setUploadId(uploadId);
            exportS3.setUploadId(uploadId);
            return this;
        }

        public Builder statsPid(long statsPid) {
            importSource.setStatsPid(statsPid);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder matchConfig(DplusMatchConfig matchConfig) {
            importSource.setMatchConfig(matchConfig);
            return this;
        }

        public DCPSourceImportWorkflowConfiguration build() {
            configuration.setContainerConfiguration(WORKFLOW_NAME, configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(importSource);
            configuration.add(exportS3);
            return configuration;
        }
    }
}
