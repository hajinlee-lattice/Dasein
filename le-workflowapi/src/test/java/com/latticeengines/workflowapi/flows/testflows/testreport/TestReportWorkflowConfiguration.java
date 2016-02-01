package com.latticeengines.workflowapi.flows.testflows.testreport;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class TestReportWorkflowConfiguration extends WorkflowConfiguration {
    public static class Builder {
        private TestReportWorkflowConfiguration testWorkflow = new TestReportWorkflowConfiguration();
        private MicroserviceStepConfiguration registerReport = new MicroserviceStepConfiguration();

        public Builder microServiceHostPort(String microServiceHostPort) {
            registerReport.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            registerReport.setCustomerSpace(customerSpace);
            return this;
        }

        public TestReportWorkflowConfiguration build() {
            testWorkflow.add(registerReport);
            return testWorkflow;
        }
    }
}
