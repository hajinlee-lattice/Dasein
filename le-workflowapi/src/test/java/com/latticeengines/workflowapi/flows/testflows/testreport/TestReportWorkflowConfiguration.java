package com.latticeengines.workflowapi.flows.testflows.testreport;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class TestReportWorkflowConfiguration extends WorkflowConfiguration {

    public static class Builder {
        private TestReportWorkflowConfiguration testWorkflow = new TestReportWorkflowConfiguration();
        private BaseReportStepConfiguration registerReport = new BaseReportStepConfiguration();

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            registerReport.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            registerReport.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            testWorkflow.setContainerConfiguration("testReportWorkflow", customerSpace, "testReportWorkflow");
            registerReport.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder reportName(String reportName) {
            registerReport.setReportNamePrefix(reportName);
            return this;
        }

        public TestReportWorkflowConfiguration build() {
            testWorkflow.add(registerReport);
            return testWorkflow;
        }
    }
}
