package com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;

public class LegacyDeleteTransactionWorkFlowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private LegacyDeleteTransactionWorkFlowConfiguration configuration =
                new LegacyDeleteTransactionWorkFlowConfiguration();
        private LegacyDeleteByUploadStepConfiguration legacyDeleteByUploadStepConfiguration =
                new LegacyDeleteByUploadStepConfiguration();
        private LegacyDeleteByDateRangeStepConfiguration legacyDeleteByDateRangeStepConfiguration =
                new LegacyDeleteByDateRangeStepConfiguration();

        public Builder Customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            legacyDeleteByUploadStepConfiguration.setCustomerSpace(customerSpace);
            legacyDeleteByDateRangeStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            legacyDeleteByUploadStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            legacyDeleteByDateRangeStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public LegacyDeleteTransactionWorkFlowConfiguration build() {
            configuration.setContainerConfiguration("legacyDeleteTransactionWorkFlow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            legacyDeleteByUploadStepConfiguration.setEntity(BusinessEntity.Transaction);
            legacyDeleteByDateRangeStepConfiguration.setEntity(BusinessEntity.Transaction);
            configuration.add(legacyDeleteByUploadStepConfiguration);
            configuration.add(legacyDeleteByDateRangeStepConfiguration);
            return configuration;
        }
    }
}
