package com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;

public class LegacyDeleteWorkFlowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private LegacyDeleteWorkFlowConfiguration configuration = new LegacyDeleteWorkFlowConfiguration();
        private LegacyDeleteAccountWorkFlowConfiguration.Builder legacyDeleteAccountWorkFlowBuilder =
                new LegacyDeleteAccountWorkFlowConfiguration.Builder();
        private LegacyDeleteContactWorkFlowConfiguratiion.Builder legacyDeleteContactWorkFlowBuilder =
                new LegacyDeleteContactWorkFlowConfiguratiion.Builder();
        private LegacyDeleteTransactionWorkFlowConfiguration.Builder legacyDeleteTransactionWorkFlowBuilder =
                new LegacyDeleteTransactionWorkFlowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            legacyDeleteAccountWorkFlowBuilder.Customer(customerSpace);
            legacyDeleteContactWorkFlowBuilder.Customer(customerSpace);
            legacyDeleteTransactionWorkFlowBuilder.Customer(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            legacyDeleteAccountWorkFlowBuilder.internalResourceHostPort(internalResourceHostPort);
            legacyDeleteContactWorkFlowBuilder.internalResourceHostPort(internalResourceHostPort);
            legacyDeleteTransactionWorkFlowBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public LegacyDeleteWorkFlowConfiguration build() {
            configuration.setContainerConfiguration("legacyDeleteWorkFlow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(legacyDeleteAccountWorkFlowBuilder.build());
            configuration.add(legacyDeleteContactWorkFlowBuilder.build());
            configuration.add(legacyDeleteTransactionWorkFlowBuilder.build());
            return configuration;
        }
    }
}
