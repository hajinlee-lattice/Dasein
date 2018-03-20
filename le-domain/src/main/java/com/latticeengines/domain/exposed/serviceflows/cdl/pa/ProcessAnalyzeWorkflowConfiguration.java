package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.RedshiftPublishWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CombineStatisticsConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSPythonBatchConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class ProcessAnalyzeWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Scoring.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {

        private ProcessAnalyzeWorkflowConfiguration configuration = new ProcessAnalyzeWorkflowConfiguration();
        private ProcessStepConfiguration processStepConfiguration = new ProcessStepConfiguration();

        private ProcessAccountWorkflowConfiguration.Builder processAccountWorkflowBuilder = new ProcessAccountWorkflowConfiguration.Builder();
        private ProcessContactWorkflowConfiguration.Builder processContactWorkflowBuilder = new ProcessContactWorkflowConfiguration.Builder();
        private ProcessProductWorkflowConfiguration.Builder processProductWorkflowBuilder = new ProcessProductWorkflowConfiguration.Builder();
        private ProcessTransactionWorkflowConfiguration.Builder processTransactionWorkflowBuilder = new ProcessTransactionWorkflowConfiguration.Builder();
        private ProcessRatingWorkflowConfiguration.Builder processRatingWorkflowBuilder = new ProcessRatingWorkflowConfiguration.Builder();

        private CombineStatisticsConfiguration combineStatisticsConfiguration = new CombineStatisticsConfiguration();
        private RedshiftPublishWorkflowConfiguration.Builder redshiftPublishWorkflowConfigurationBuilder = new RedshiftPublishWorkflowConfiguration.Builder();
        private AWSPythonBatchConfiguration awsPythonDataConfiguration = new AWSPythonBatchConfiguration();

        public Builder initialDataFeedStatus(DataFeed.Status initialDataFeedStatus) {
            processStepConfiguration.setInitialDataFeedStatus(initialDataFeedStatus);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processStepConfiguration.setCustomerSpace(customerSpace);
            processAccountWorkflowBuilder.customer(customerSpace);
            processContactWorkflowBuilder.customer(customerSpace);
            processProductWorkflowBuilder.customer(customerSpace);
            processTransactionWorkflowBuilder.customer(customerSpace);
            processRatingWorkflowBuilder.customer(customerSpace);
            combineStatisticsConfiguration.setCustomerSpace(customerSpace);
            redshiftPublishWorkflowConfigurationBuilder.customer(customerSpace);
            awsPythonDataConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            processStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            redshiftPublishWorkflowConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            processRatingWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            awsPythonDataConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            processStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processAccountWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processContactWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processProductWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processTransactionWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processRatingWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            redshiftPublishWorkflowConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            awsPythonDataConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder hdfsToRedshiftConfiguration(HdfsToRedshiftConfiguration hdfsToRedshiftConfiguration) {
            redshiftPublishWorkflowConfigurationBuilder.hdfsToRedshiftConfiguration(hdfsToRedshiftConfiguration);
            processRatingWorkflowBuilder.hdfsToRedshiftConfiguration(hdfsToRedshiftConfiguration);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder workflowContainerMem(int mb) {
            configuration.setContainerMemoryMB(mb);
            return this;
        }

        public Builder importAndDeleteJobIds(List<Long> importJobIds) {
            processStepConfiguration.setImportAndDeleteJobIds(importJobIds);
            return this;
        }

        public Builder actionIds(List<Long> actionIds) {
            processStepConfiguration.setActionIds(actionIds);
            return this;
        }

        public Builder currentDataCloudBuildNumber(String currentDataCloudBuildNumber) {
            processStepConfiguration.setDataCloudBuildNumber(currentDataCloudBuildNumber);
            return this;
        }

        public Builder dataCloudVersion(DataCloudVersion dataCloudVersion) {
            processRatingWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            processAccountWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchYarnQueue(String matchYarnQueue) {
            processRatingWorkflowBuilder.matchYarnQueue(matchYarnQueue);
            return this;
        }

        public Builder fetchOnly(boolean fetchOnly) {
            processRatingWorkflowBuilder.fetchOnly(fetchOnly);
            return this;
        }

        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            processAccountWorkflowBuilder.rebuildEntities(entities);
            processContactWorkflowBuilder.rebuildEntities(entities);
            processProductWorkflowBuilder.rebuildEntities(entities);
            processTransactionWorkflowBuilder.rebuildEntities(entities);
            processRatingWorkflowBuilder.rebuildEntities(entities);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            return this;
        }

        public ProcessAnalyzeWorkflowConfiguration build() {
            configuration.setContainerConfiguration("processAnalyzeWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(processStepConfiguration);
            configuration.add(processAccountWorkflowBuilder.build());
            configuration.add(processContactWorkflowBuilder.build());
            configuration.add(processProductWorkflowBuilder.build());
            configuration.add(processTransactionWorkflowBuilder.build());
            configuration.add(processRatingWorkflowBuilder.build());
            configuration.add(combineStatisticsConfiguration);
            configuration.add(redshiftPublishWorkflowConfigurationBuilder.build());
            configuration.add(awsPythonDataConfiguration);
            return configuration;
        }
    }
}
