package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateIntentAlertArtifactsStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.SendIntentAlertEmailStepConfiguration;

public class GenerateIntentEmailAlertWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String NAME = "GenerateIntentEmailAlertWorkflowConfiguration";
    public static final String WORKFLOW_NAME = "generateIntentEmailAlertWorkflow";

    public static class Builder {
        private GenerateIntentEmailAlertWorkflowConfiguration configuration = new GenerateIntentEmailAlertWorkflowConfiguration();
        private GenerateIntentAlertArtifactsStepConfiguration generateIntentArtifacts = new GenerateIntentAlertArtifactsStepConfiguration();
        private SendIntentAlertEmailStepConfiguration sendIntentAlertEmail = new SendIntentAlertEmailStepConfiguration();

        public GenerateIntentEmailAlertWorkflowConfiguration.Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            generateIntentArtifacts.setCustomerSpace(customerSpace);
            sendIntentAlertEmail.setCustomerSpace(customerSpace);
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
            sendIntentAlertEmail.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public GenerateIntentEmailAlertWorkflowConfiguration build() {
            configuration.setContainerConfiguration(WORKFLOW_NAME, configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(generateIntentArtifacts);
            configuration.add(sendIntentAlertEmail);
            return configuration;
        }

    }
}
