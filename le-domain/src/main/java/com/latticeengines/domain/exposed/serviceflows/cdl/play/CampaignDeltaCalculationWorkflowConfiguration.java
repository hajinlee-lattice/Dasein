package com.latticeengines.domain.exposed.serviceflows.cdl.play;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class CampaignDeltaCalculationWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private CampaignDeltaCalculationWorkflowConfiguration configuration = new CampaignDeltaCalculationWorkflowConfiguration();
        private ImportExportS3StepConfiguration importS3 = new ImportExportS3StepConfiguration();
        private CalculateDeltaStepConfiguration calculateDeltaStepConfiguration = new CalculateDeltaStepConfiguration();
        private QueuePlayLaunchesStepConfiguration queuePlayLaunchesStepConfiguration = new QueuePlayLaunchesStepConfiguration();

        public CampaignDeltaCalculationWorkflowConfiguration.Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("campaignDeltaCalculationWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            importS3.setCustomerSpace(customerSpace);
            calculateDeltaStepConfiguration.setCustomerSpace(customerSpace);
            queuePlayLaunchesStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder workflow(String workflowName) {
            configuration.setWorkflowName(workflowName);
            configuration.setName(workflowName);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder dataCollectionVersion(
                DataCollection.Version version) {
            importS3.setVersion(version);
            calculateDeltaStepConfiguration.setVersion(version);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder playId(String playId) {
            queuePlayLaunchesStepConfiguration.setPlayId(playId);
            calculateDeltaStepConfiguration.setPlayId(playId);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder channelId(String channelId) {
            queuePlayLaunchesStepConfiguration.setChannelId(channelId);
            calculateDeltaStepConfiguration.setChannelId(channelId);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder inputProperties(
                Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration build() {
            configuration.add(importS3);
            configuration.add(calculateDeltaStepConfiguration);
            configuration.add(queuePlayLaunchesStepConfiguration);
            return configuration;
        }
    }
}
