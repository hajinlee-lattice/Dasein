package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CalculatePurchaseHistoryConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CalculateStatsStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.SortContactStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.SortProductStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.UpdateStatsObjectsConfiguration;

public class ProfileAndPublishWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    private ProfileAndPublishWorkflowConfiguration() {
    }

    public static class Builder {

        private ProfileAndPublishWorkflowConfiguration configuration = new ProfileAndPublishWorkflowConfiguration();
        private CalculateStatsStepConfiguration calculateStatsConfiguration = new CalculateStatsStepConfiguration();
        private UpdateStatsObjectsConfiguration updateStatsObjectsConfiguration = new UpdateStatsObjectsConfiguration();
        private SortContactStepConfiguration sortContactConfiguration = new SortContactStepConfiguration();
        private SortProductStepConfiguration sortProductStepConfiguration = new SortProductStepConfiguration();
        private CalculatePurchaseHistoryConfiguration calculatePurchaseHistoryConfiguration = new CalculatePurchaseHistoryConfiguration();
        private RedshiftPublishWorkflowConfiguration.Builder redshiftPublishWorkflowConfigurationBuilder = new RedshiftPublishWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("profileAndPublishWorkflow", customerSpace,
                    "profileAndPublishWorkflow");
            calculateStatsConfiguration.setCustomerSpace(customerSpace);
            sortContactConfiguration.setCustomerSpace(customerSpace);
            sortProductStepConfiguration.setCustomerSpace(customerSpace);
            calculatePurchaseHistoryConfiguration.setCustomerSpace(customerSpace);
            updateStatsObjectsConfiguration.setCustomerSpace(customerSpace);
            redshiftPublishWorkflowConfigurationBuilder.customer(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            redshiftPublishWorkflowConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
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

        public Builder workflowContainerMem(int mb) {
            configuration.setContainerMemoryMB(mb);
            return this;
        }

        public ProfileAndPublishWorkflowConfiguration build() {
            configuration.add(calculateStatsConfiguration);
            configuration.add(updateStatsObjectsConfiguration);
            configuration.add(sortContactConfiguration);
            configuration.add(sortProductStepConfiguration);
            configuration.add(calculatePurchaseHistoryConfiguration);
            configuration.add(redshiftPublishWorkflowConfigurationBuilder.build());
            return configuration;
        }

    }
}
