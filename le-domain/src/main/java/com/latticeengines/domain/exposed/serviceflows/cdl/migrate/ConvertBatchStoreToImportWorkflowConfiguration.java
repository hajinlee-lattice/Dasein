package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.BaseConvertBatchStoreServiceConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.RegisterImportActionStepConfiguration;

public class ConvertBatchStoreToImportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public ConvertBatchStoreToImportWorkflowConfiguration() {

    }

    public static class Builder {
        private ConvertBatchStoreToImportWorkflowConfiguration configuration =
                new ConvertBatchStoreToImportWorkflowConfiguration();

        private ConvertBatchStoreStepConfiguration convertBatchStoreStepConfiguration = new ConvertBatchStoreStepConfiguration();
        private RegisterImportActionStepConfiguration registerImportActionStepConfiguration = new RegisterImportActionStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            convertBatchStoreStepConfiguration.setCustomerSpace(customerSpace);
            registerImportActionStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            convertBatchStoreStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            registerImportActionStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            registerImportActionStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            return this;
        }

        public Builder convertBatchStoreServiceConfiguration(BaseConvertBatchStoreServiceConfiguration convertBatchStoreServiceConfiguration) {
            convertBatchStoreStepConfiguration.setConvertServiceConfig(convertBatchStoreServiceConfiguration);
            registerImportActionStepConfiguration.setConvertServiceConfig(convertBatchStoreServiceConfiguration);
            return this;
        }

        public Builder entity(BusinessEntity entity) {
            convertBatchStoreStepConfiguration.setEntity(entity);
            registerImportActionStepConfiguration.setEntity(entity);
            return this;
        }

        public Builder actionPid(Long actionPid) {
            registerImportActionStepConfiguration.setActionPid(actionPid);
            return this;
        }

        public Builder skipConvert(boolean skipConvert) {
            convertBatchStoreStepConfiguration.setSkipStep(skipConvert);
            registerImportActionStepConfiguration.setSkipStep(skipConvert);
            return this;
        }

        public ConvertBatchStoreToImportWorkflowConfiguration build() {
            configuration.setContainerConfiguration("convertBatchStoreToImportWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(convertBatchStoreStepConfiguration);
            configuration.add(registerImportActionStepConfiguration);
            return configuration;
        }
    }
}
