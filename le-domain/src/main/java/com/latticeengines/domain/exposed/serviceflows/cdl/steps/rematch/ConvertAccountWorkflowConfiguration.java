package com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreToImportServiceConfiguration;

public class ConvertAccountWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private ConvertAccountWorkflowConfiguration configuration =
                new ConvertAccountWorkflowConfiguration();
        private ConvertBatchStoreToDataTableConfiguration convertBatchStoreToDataTableConfiguration =
                new ConvertBatchStoreToDataTableConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            convertBatchStoreToDataTableConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder setEntity() {
            convertBatchStoreToDataTableConfiguration.setEntity(BusinessEntity.Account);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            convertBatchStoreToDataTableConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder setSkipStep(boolean skipStep) {
            convertBatchStoreToDataTableConfiguration.setSkipStep(skipStep);
            return this;
        }

        public Builder setConvertServiceConfig() {
            ConvertBatchStoreToImportServiceConfiguration serviceConfiguration = new ConvertBatchStoreToImportServiceConfiguration();
            serviceConfiguration.setEntity(BusinessEntity.Account);
            convertBatchStoreToDataTableConfiguration.setConvertServiceConfig(serviceConfiguration);
            return this;
        }

        public ConvertAccountWorkflowConfiguration build() {
            configuration.setContainerConfiguration("convertAccountWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(convertBatchStoreToDataTableConfiguration);
            return configuration;
        }
    }
}
