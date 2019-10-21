package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.DeleteFileUploadStepConfiguration;

public class RegisterDeleteDataWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public RegisterDeleteDataWorkflowConfiguration() {
    }

    public static class Builder {
        private RegisterDeleteDataWorkflowConfiguration configuration = new RegisterDeleteDataWorkflowConfiguration();
        private DeleteFileUploadStepConfiguration deleteFileUploadStepConfiguration = new DeleteFileUploadStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            deleteFileUploadStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            deleteFileUploadStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            deleteFileUploadStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
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
            return configuration;
        }
    }
}
