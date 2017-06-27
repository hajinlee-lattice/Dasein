package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CalculateStatsStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.UpdateStatsObjectsConfiguration;

public class CalculateStatsWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    private CalculateStatsWorkflowConfiguration() {
    }

    public static class Builder {

        private CalculateStatsWorkflowConfiguration configuration = new CalculateStatsWorkflowConfiguration();
        private CalculateStatsStepConfiguration calculateStatsConfiguration = new CalculateStatsStepConfiguration();
        private UpdateStatsObjectsConfiguration updateStatsObjectsConfiguration = new UpdateStatsObjectsConfiguration();
        private RedshiftPublishWorkflowConfiguration.Builder redshiftPublishWorkflowConfigurationBuilder = new RedshiftPublishWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("calculateStatsWorkflow", customerSpace, "calculateStatsWorkflow");
            calculateStatsConfiguration.setCustomerSpace(customerSpace);
            updateStatsObjectsConfiguration.setCustomerSpace(customerSpace);
            redshiftPublishWorkflowConfigurationBuilder.customer(customerSpace);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder hdfsToRedshiftConfiguration(HdfsToRedshiftConfiguration createExportBaseConfig) {
            redshiftPublishWorkflowConfigurationBuilder.hdfsToRedshiftConfiguration(createExportBaseConfig);
            return this;
        }

        public Builder microServiceHostPort(String microserviceHostPort) {
            redshiftPublishWorkflowConfigurationBuilder.microServiceHostPort(microserviceHostPort);
            return this;
        }

        public CalculateStatsWorkflowConfiguration build() {
            configuration.add(calculateStatsConfiguration);
            configuration.add(updateStatsObjectsConfiguration);
            configuration.add(redshiftPublishWorkflowConfigurationBuilder.build());
            return configuration;
        }

    }
}
