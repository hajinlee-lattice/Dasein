package com.latticeengines.domain.exposed.serviceflows.core.spark;

import java.util.Collection;
import java.util.Collections;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.RunSparkWorkflowRequest;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class RunSparkWorkflowConfig extends WorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        // lightest package
        return Collections.singleton(SoftwareLibrary.Scoring.getName());
    }

    private RunSparkWorkflowConfig() {
    }

    public static class Builder {
        private RunSparkWorkflowConfig configuration = new RunSparkWorkflowConfig();
        private RunSparkWorkflowStepConfig step = new RunSparkWorkflowStepConfig();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            step.setCustomerSpace(customerSpace.toString());
            return this;
        }

        public Builder request(RunSparkWorkflowRequest request) {
            step.setRequest(request);
            return this;
        }

        public RunSparkWorkflowConfig build() {
            configuration.setContainerConfiguration("runSparkWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(step);
            return configuration;
        }

    }

}
