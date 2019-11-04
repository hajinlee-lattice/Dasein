package com.latticeengines.domain.exposed.serviceflows.cdl.play;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;

public class CampaignDeltaCalculationWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private CampaignDeltaCalculationWorkflowConfiguration configuration = new CampaignDeltaCalculationWorkflowConfiguration();
        private ImportDeltaArtifactsFromS3Configuration importDeltaArtifactsFromS3Configuration = new ImportDeltaArtifactsFromS3Configuration();
        private GenerateLaunchUniverseStepConfiguration generateLaunchUniverseStepConfiguration = new GenerateLaunchUniverseStepConfiguration();
        private GenerateLaunchArtifactsStepConfiguration generateLaunchArtifactsStepConfiguration = new GenerateLaunchArtifactsStepConfiguration();
        private CalculateDeltaStepConfiguration calculateDeltaStepConfiguration = new CalculateDeltaStepConfiguration();
        private ExportDeltaArtifactsToS3StepConfiguration exportDeltaArtifactsToS3StepConfiguration = new ExportDeltaArtifactsToS3StepConfiguration();
        private QueuePlayLaunchesStepConfiguration queuePlayLaunchesStepConfiguration = new QueuePlayLaunchesStepConfiguration();

        public CampaignDeltaCalculationWorkflowConfiguration.Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("campaignDeltaCalculationWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            importDeltaArtifactsFromS3Configuration.setCustomerSpace(customerSpace);
            generateLaunchUniverseStepConfiguration.setCustomerSpace(customerSpace);
            generateLaunchArtifactsStepConfiguration.setCustomerSpace(customerSpace);
            calculateDeltaStepConfiguration.setCustomerSpace(customerSpace);
            queuePlayLaunchesStepConfiguration.setCustomerSpace(customerSpace);
            exportDeltaArtifactsToS3StepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder workflow(String workflowName) {
            configuration.setWorkflowName(workflowName);
            configuration.setName(workflowName);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder dataCollectionVersion(
                DataCollection.Version version) {
            importDeltaArtifactsFromS3Configuration.setVersion(version);
            generateLaunchUniverseStepConfiguration.setVersion(version);
            generateLaunchArtifactsStepConfiguration.setVersion(version);
            calculateDeltaStepConfiguration.setVersion(version);
            exportDeltaArtifactsToS3StepConfiguration.setVersion(version);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder playId(String playId) {
            importDeltaArtifactsFromS3Configuration.setPlayId(playId);
            generateLaunchUniverseStepConfiguration.setPlayId(playId);
            generateLaunchArtifactsStepConfiguration.setPlayId(playId);
            calculateDeltaStepConfiguration.setPlayId(playId);
            queuePlayLaunchesStepConfiguration.setPlayId(playId);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder channelId(String channelId) {
            importDeltaArtifactsFromS3Configuration.setChannelId(channelId);
            generateLaunchUniverseStepConfiguration.setChannelId(channelId);
            generateLaunchArtifactsStepConfiguration.setChannelId(channelId);
            calculateDeltaStepConfiguration.setChannelId(channelId);
            queuePlayLaunchesStepConfiguration.setChannelId(channelId);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder launchId(String launchId) {
            generateLaunchUniverseStepConfiguration.setLaunchId(launchId);
            generateLaunchArtifactsStepConfiguration.setLaunchId(launchId);
            calculateDeltaStepConfiguration.setLaunchId(launchId);
            queuePlayLaunchesStepConfiguration.setLaunchId(launchId);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder executionId(String executionId) {
            importDeltaArtifactsFromS3Configuration.setExecutionId(executionId);
            generateLaunchUniverseStepConfiguration.setExecutionId(executionId);
            generateLaunchArtifactsStepConfiguration.setExecutionId(executionId);
            calculateDeltaStepConfiguration.setExecutionId(executionId);
            queuePlayLaunchesStepConfiguration.setExecutionId(executionId);
            exportDeltaArtifactsToS3StepConfiguration.setExecutionId(executionId);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration.Builder inputProperties(
                Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public CampaignDeltaCalculationWorkflowConfiguration build() {
            configuration.add(importDeltaArtifactsFromS3Configuration);
            configuration.add(generateLaunchUniverseStepConfiguration);
            configuration.add(generateLaunchArtifactsStepConfiguration);
            configuration.add(calculateDeltaStepConfiguration);
            configuration.add(exportDeltaArtifactsToS3StepConfiguration);
            configuration.add(queuePlayLaunchesStepConfiguration);
            return configuration;
        }

    }
}
