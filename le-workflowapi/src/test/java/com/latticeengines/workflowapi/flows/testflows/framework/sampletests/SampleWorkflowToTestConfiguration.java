package com.latticeengines.workflowapi.flows.testflows.framework.sampletests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class SampleWorkflowToTestConfiguration extends WorkflowConfiguration {

    private static final Logger log = LoggerFactory.getLogger(SampleWorkflowToTestConfiguration.class);

    public static class Builder {
        private SampleWorkflowToTestConfiguration configuration = new SampleWorkflowToTestConfiguration();
        private SampleWorkflowToTestStep1Configuration sampleWorkflowToTestStep1Configuration =
                new SampleWorkflowToTestStep1Configuration();
        private SampleWorkflowToTestStep2Configuration sampleWorkflowToTestStep2Configuration =
                new SampleWorkflowToTestStep2Configuration();


        public Builder workflow(String workflowName) {
            log.error("In SampleWorkflowToTestConfiguration.Builder.workflow");
            configuration.setWorkflowName(workflowName);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            log.error("In SampleWorkflowToTestConfiguration.Builder.customer");
            configuration.setCustomerSpace(customerSpace);
            sampleWorkflowToTestStep1Configuration.setCustomerSpace(customerSpace.toString());
            sampleWorkflowToTestStep2Configuration.setCustomerSpace(customerSpace.toString());
            return this;
        }

        public SampleWorkflowToTestConfiguration build() {
            log.error("In SampleWorkflowToTestConfiguration.Builder.build");
            configuration.setContainerConfiguration(configuration.getWorkflowName(),
                    configuration.getCustomerSpace(), configuration.getWorkflowName());
            return configuration;
        }
    }
}
