package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.DeleteFileUploadStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class RegisterDeleteDataWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public RegisterDeleteDataWorkflowConfiguration() {
    }

    public static class Builder {
        private RegisterDeleteDataWorkflowConfiguration configuration = new RegisterDeleteDataWorkflowConfiguration();
        private DeleteFileUploadStepConfiguration deleteFileUploadStepConfiguration = new DeleteFileUploadStepConfiguration();
        private ImportExportS3StepConfiguration exportToS3 = new ImportExportS3StepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            deleteFileUploadStepConfiguration.setCustomerSpace(customerSpace);
            exportToS3.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            deleteFileUploadStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            exportToS3.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            deleteFileUploadStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            exportToS3.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            return this;
        }

        public Builder tableName(String tableName) {
            deleteFileUploadStepConfiguration.setTableName(tableName);
            return this;
        }

        public Builder filePath(String filePath) {
            deleteFileUploadStepConfiguration.setFilePath(filePath);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public RegisterDeleteDataWorkflowConfiguration build() {
            configuration.setContainerConfiguration("registerDeleteDataWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(deleteFileUploadStepConfiguration);
            configuration.add(exportToS3);
            return configuration;
        }
    }
}
