package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CalculateStatsStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.UpdateStatsObjectsConfiguration;
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

        public Builder dataCollectionType(DataCollectionType dataCollectionType) {
            calculateStatsConfiguration.setDataCollectionType(dataCollectionType);
            updateStatsObjectsConfiguration.setDataCollectionType(dataCollectionType);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public CalculateStatsWorkflowConfiguration build() {
            configuration.add(calculateStatsConfiguration);
            configuration.add(updateStatsObjectsConfiguration);
            return configuration;
        }

    }
}
