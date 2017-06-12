package com.latticeengines.cdl.workflow;

import com.latticeengines.cdl.workflow.steps.CalculateStatsStepConfiguration;
import com.latticeengines.cdl.workflow.steps.UpdateStatsObjectsConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class CalculateStatsWorkflowConfiguration extends WorkflowConfiguration {

    private CalculateStatsWorkflowConfiguration() {
    }

    public static class Builder {

        private CalculateStatsWorkflowConfiguration configuration = new CalculateStatsWorkflowConfiguration();
        private CalculateStatsStepConfiguration calculateStatsConfiguration = new CalculateStatsStepConfiguration();
        private UpdateStatsObjectsConfiguration updateStatsObjectsConfiguration = new UpdateStatsObjectsConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("calculateStatsWorkflow", customerSpace, "calculateStatsWorkflow");
            calculateStatsConfiguration.setCustomerSpace(customerSpace);
            updateStatsObjectsConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder masterTableName(String masterTableName) {
            calculateStatsConfiguration.setMasterTableName(masterTableName);
            updateStatsObjectsConfiguration.setMasterTableName(masterTableName);
            return this;
        }

        public CalculateStatsWorkflowConfiguration build() {
            configuration.add(calculateStatsConfiguration);
            configuration.add(updateStatsObjectsConfiguration);
            return configuration;
        }

    }
}
