package com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;

public class LegacyDeleteContactWorkFlowConfiguratiion extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private LegacyDeleteContactWorkFlowConfiguratiion configuration =
                new LegacyDeleteContactWorkFlowConfiguratiion();
        private LegacyDeleteStepConfiguration legacyDeleteSparkStepConfiguration =
                new LegacyDeleteStepConfiguration();

        public Builder Customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            legacyDeleteSparkStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            legacyDeleteSparkStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder entityMatchGAOnly(boolean gaOnly) {
            legacyDeleteSparkStepConfiguration.setEntityMatchGAEnabled(gaOnly);
            return this;
        }

        public LegacyDeleteContactWorkFlowConfiguratiion build() {
            configuration.setContainerConfiguration("legacyDeleteContactWorkFlow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            legacyDeleteSparkStepConfiguration.setEntity(BusinessEntity.Contact);
            configuration.add(legacyDeleteSparkStepConfiguration);
            return configuration;
        }
    }
}
