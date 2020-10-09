package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateIntentAlertArtifactsStepConfiguration;

public class GenerateIntentEmailAlertWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String NAME = "GenerateIntentEmailAlertWorkflowConfiguration";
    public static final String WORKFLOW_NAME = "generateIntentEmailAlertWorkflow";

    public static class Builder {
        private GenerateIntentEmailAlertWorkflowConfiguration configuration = new GenerateIntentEmailAlertWorkflowConfiguration();
        private GenerateIntentAlertArtifactsStepConfiguration generateIntentArtifacts = new GenerateIntentAlertArtifactsStepConfiguration();

        public GenerateIntentEmailAlertWorkflowConfiguration.Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            generateIntentArtifacts.setCustomerSpace(customerSpace);
            return this;
        }

        public GenerateIntentEmailAlertWorkflowConfiguration.Builder workflow(String workflowName) {
            configuration.setWorkflowName(workflowName);
            configuration.setName(workflowName);
            return this;
        }

        public GenerateIntentEmailAlertWorkflowConfiguration.Builder internalResourceHostPort(
                String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            generateIntentArtifacts.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public GenerateIntentEmailAlertWorkflowConfiguration build() {
            configuration.setContainerConfiguration(WORKFLOW_NAME, configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(generateIntentArtifacts);
            return configuration;
        }

    }
}
