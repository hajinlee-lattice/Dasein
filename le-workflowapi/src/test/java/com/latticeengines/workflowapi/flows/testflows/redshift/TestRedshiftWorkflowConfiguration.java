package com.latticeengines.workflowapi.flows.testflows.redshift;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToRedshiftStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class TestRedshiftWorkflowConfiguration extends WorkflowConfiguration {

    public static class Builder {
        private TestRedshiftWorkflowConfiguration configuration = new TestRedshiftWorkflowConfiguration();
        private PrepareTestRedshiftConfiguration prepare = new PrepareTestRedshiftConfiguration();
        private ExportToRedshiftStepConfiguration export = new ExportToRedshiftStepConfiguration();

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            export.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            export.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("testRedshiftWorkflow", customerSpace, "testRedshiftWorkflow");
            export.setCustomerSpace(customerSpace);
            prepare.setCustomerSpace(customerSpace.toString());
            return this;
        }

        public Builder updateMode(boolean updateMode) {
            prepare.setUpdateMode(updateMode);
            return this;
        }

        public TestRedshiftWorkflowConfiguration build() {
            configuration.add(prepare);
            configuration.add(export);
            return configuration;
        }
    }
}
