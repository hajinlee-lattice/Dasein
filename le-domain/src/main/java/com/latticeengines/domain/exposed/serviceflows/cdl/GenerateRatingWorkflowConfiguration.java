package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;


public class GenerateRatingWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    private GenerateRatingWorkflowConfiguration() {

    }

    public static class Builder {

        public GenerateRatingWorkflowConfiguration configuration = new GenerateRatingWorkflowConfiguration();

        private GenerateRatingStepConfiguration generateRatingStepConfiguration = new GenerateRatingStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("processAnalyzeWorkflow", customerSpace,
                    "processAnalyzeWorkflow");
            generateRatingStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            generateRatingStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public GenerateRatingWorkflowConfiguration build() {
            configuration.add(generateRatingStepConfiguration);
            return configuration;
        }
    }
}
