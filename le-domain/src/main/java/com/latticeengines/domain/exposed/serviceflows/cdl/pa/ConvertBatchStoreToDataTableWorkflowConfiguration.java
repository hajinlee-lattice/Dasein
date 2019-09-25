package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.ConvertAccountWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.ConvertContactWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.ConvertTransactionWorkflowConfiguration;

public class ConvertBatchStoreToDataTableWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private ConvertBatchStoreToDataTableWorkflowConfiguration configuration =
                new ConvertBatchStoreToDataTableWorkflowConfiguration();
        private ConvertAccountWorkflowConfiguration.Builder convertAccountWorkflowBuilder =
                new ConvertAccountWorkflowConfiguration.Builder();
        private ConvertContactWorkflowConfiguration.Builder convertContactWorkflowBuilder =
                new ConvertContactWorkflowConfiguration.Builder();
        private ConvertTransactionWorkflowConfiguration.Builder convertTransactionWorkflowBuilder =
                new ConvertTransactionWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            convertAccountWorkflowBuilder.customer(customerSpace);
            convertContactWorkflowBuilder.customer(customerSpace);
            convertTransactionWorkflowBuilder.customer(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            convertAccountWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            convertContactWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            convertTransactionWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder setSkipStep(boolean skipStep) {
            convertAccountWorkflowBuilder.setSkipStep(skipStep);
            convertContactWorkflowBuilder.setSkipStep(skipStep);
            convertTransactionWorkflowBuilder.setSkipStep(skipStep);
            return this;
        }

        public Builder setConvertServiceConfig() {
            convertAccountWorkflowBuilder.setConvertServiceConfig();
            convertContactWorkflowBuilder.setConvertServiceConfig();
            convertTransactionWorkflowBuilder.setConvertServiceConfig();
            return this;
        }

        public ConvertBatchStoreToDataTableWorkflowConfiguration build() {
            configuration.setContainerConfiguration("convertBatchStoreToDataTableWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(convertAccountWorkflowBuilder.build());
            configuration.add(convertContactWorkflowBuilder.build());
            configuration.add(convertTransactionWorkflowBuilder.build());
            return configuration;
        }
    }
}
