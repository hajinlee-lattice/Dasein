package com.latticeengines.domain.exposed.serviceflows.cdl.play;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;

public class CampaignDeltaCalculationWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private CampaignDeltaCalculationWorkflowConfiguration configuration = new CampaignDeltaCalculationWorkflowConfiguration();
        private QueuePlayLaunchesStepConfiguration queuePlayLaunchesStepConfiguration = new QueuePlayLaunchesStepConfiguration();

        public CampaignDeltaCalculationWorkflowConfiguration.Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("campaignDeltaCalculationWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            queuePlayLaunchesStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder workflow(String workflowName) {
            configuration.setWorkflowName(workflowName);
            configuration.setName(workflowName);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder playId(String playId) {
            queuePlayLaunchesStepConfiguration.setPlayId(playId);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder channelId(String channelId) {
            queuePlayLaunchesStepConfiguration.setChannelId(channelId);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder inputProperties(
                Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration build() {
            configuration.add(queuePlayLaunchesStepConfiguration);
            return configuration;
        }
    }
}
