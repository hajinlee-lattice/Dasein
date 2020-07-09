package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.AccountLookupToDynamoStepConfiguration;

public class PublishAccountLookupWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String NAME = "PublishAccountLookupWorkflowConfiguration";

    public static class Builder {

        private final PublishAccountLookupWorkflowConfiguration config = new PublishAccountLookupWorkflowConfiguration();
        private final AccountLookupToDynamoStepConfiguration accountLookupToDynamoStepConfiguration = new AccountLookupToDynamoStepConfiguration();

        public Builder customeSpace(CustomerSpace customerSpace) {
            config.setCustomerSpace(customerSpace);
            accountLookupToDynamoStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            config.setInternalResourceHostPort(internalResourceHostPort);
            accountLookupToDynamoStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            accountLookupToDynamoStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder dynamoSignature(String signature) {
            accountLookupToDynamoStepConfiguration.setDynamoSignature(signature);
            return this;
        }

        public PublishAccountLookupWorkflowConfiguration build() {
            config.setContainerConfiguration("PublishAccountLookupWorkflow", config.getCustomerSpace(), config.getClass().getSimpleName());
            config.add(accountLookupToDynamoStepConfiguration);
            return config;
        }
    }
}
