package com.latticeengines.workflowapi.flows.testflows.dynamo;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToDynamoStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class TestDynamoWorkflowConfiguration extends WorkflowConfiguration {

    public static class Builder {
        private TestDynamoWorkflowConfiguration configuration = new TestDynamoWorkflowConfiguration();
        private PrepareTestDynamoConfiguration prepare = new PrepareTestDynamoConfiguration();
        private ExportToDynamoStepConfiguration export = new ExportToDynamoStepConfiguration();

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            export.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            export.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("testDynamoWorkflow", customerSpace, "testDynamoWorkflow");
            prepare.setCustomerSpace(customerSpace.toString());
            export.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder dynamoSignature(String signature) {
            export.setDynamoSignature(signature);
            return this;
        }

        public Builder updateMode(boolean updateMode) {
            prepare.setUpdateMode(updateMode);
            return this;
        }

        public TestDynamoWorkflowConfiguration build() {
            configuration.add(prepare);
            configuration.add(export);
            return configuration;
        }
    }
}
