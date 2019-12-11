package com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;

public class LegacyDeleteAccountWorkFlowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private LegacyDeleteAccountWorkFlowConfiguration configuration = new LegacyDeleteAccountWorkFlowConfiguration();
        private LegacyDeleteByUploadStepConfiguration legacyDeleteByUploadStepConfiguration = new LegacyDeleteByUploadStepConfiguration();

        public Builder Customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            legacyDeleteByUploadStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            legacyDeleteByUploadStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public LegacyDeleteAccountWorkFlowConfiguration build() {
            configuration.setContainerConfiguration("legacyDeleteAccountWorkFlow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            legacyDeleteByUploadStepConfiguration.setEntity(BusinessEntity.Account);
            configuration.add(legacyDeleteByUploadStepConfiguration);
            return configuration;
        }
    }
}
