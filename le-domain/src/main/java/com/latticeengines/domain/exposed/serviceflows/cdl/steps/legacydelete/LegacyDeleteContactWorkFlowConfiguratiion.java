package com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;

public class LegacyDeleteContactWorkFlowConfiguratiion extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private LegacyDeleteContactWorkFlowConfiguratiion configuration =
                new LegacyDeleteContactWorkFlowConfiguratiion();
        private LegacyDeleteByUploadStepConfiguration legacyDeleteByUploadStepConfiguration =
                new LegacyDeleteByUploadStepConfiguration();

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

        public LegacyDeleteContactWorkFlowConfiguratiion build() {
            configuration.setContainerConfiguration("legacyDeleteContactWorkFlow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            legacyDeleteByUploadStepConfiguration.setEntity(BusinessEntity.Contact);
            configuration.add(legacyDeleteByUploadStepConfiguration);
            return configuration;
        }
    }
}
