package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CombineStatisticsConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSPythonBatchConfiguration;

public class ProcessAnalyzeWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    private ProcessAnalyzeWorkflowConfiguration() {

    }

    public static class Builder {

        public ProcessAnalyzeWorkflowConfiguration configuration = new ProcessAnalyzeWorkflowConfiguration();

        private ProcessStepConfiguration processStepConfiguration = new ProcessStepConfiguration();
        private ProcessAccountStepConfiguration processAccountStepConfiguration = new ProcessAccountStepConfiguration();
        private ProcessContactStepConfiguration processContactStepConfiguration = new ProcessContactStepConfiguration();
        private ProcessProductStepConfiguration processProductStepConfiguration = new ProcessProductStepConfiguration();
        private ProcessTransactionStepConfiguration processTransactionStepConfiguration = new ProcessTransactionStepConfiguration();
        private ProcessRatingStepConfiguration processRatingStepConfiguration = new ProcessRatingStepConfiguration();
        private GenerateRatingWorkflowConfiguration.Builder generateRatingWorfklowConfigurationBuilder = new GenerateRatingWorkflowConfiguration.Builder();
        private CombineStatisticsConfiguration combineStatisticsConfiguration = new CombineStatisticsConfiguration();
        private RedshiftPublishWorkflowConfiguration.Builder redshiftPublishWorkflowConfigurationBuilder = new RedshiftPublishWorkflowConfiguration.Builder();
        private AWSPythonBatchConfiguration awsPythonDataConfiguration = new AWSPythonBatchConfiguration();

        public Builder initialDataFeedStatus(DataFeed.Status initialDataFeedStatus) {
            processStepConfiguration.setInitialDataFeedStatus(initialDataFeedStatus);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("processAnalyzeWorkflow", customerSpace, "processAnalyzeWorkflow");
            processStepConfiguration.setCustomerSpace(customerSpace);
            processAccountStepConfiguration.setCustomerSpace(customerSpace);
            processContactStepConfiguration.setCustomerSpace(customerSpace);
            processProductStepConfiguration.setCustomerSpace(customerSpace);
            processTransactionStepConfiguration.setCustomerSpace(customerSpace);
            processRatingStepConfiguration.setCustomerSpace(customerSpace);
            combineStatisticsConfiguration.setCustomerSpace(customerSpace);
            generateRatingWorfklowConfigurationBuilder.customer(customerSpace);
            redshiftPublishWorkflowConfigurationBuilder.customer(customerSpace);
            awsPythonDataConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            processStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            redshiftPublishWorkflowConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            generateRatingWorfklowConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            awsPythonDataConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            processStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processAccountStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processContactStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processProductStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processTransactionStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processRatingStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            redshiftPublishWorkflowConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            awsPythonDataConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder hdfsToRedshiftConfiguration(HdfsToRedshiftConfiguration hdfsToRedshiftConfiguration) {
            redshiftPublishWorkflowConfigurationBuilder.hdfsToRedshiftConfiguration(hdfsToRedshiftConfiguration);
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
            generateRatingWorfklowConfigurationBuilder.dataCloudVersion(dataCloudVersion);
            processAccountStepConfiguration.setDataCloudVersion(dataCloudVersion.getVersion());
            return this;
        }

        public Builder matchYarnQueue(String matchYarnQueue) {
            generateRatingWorfklowConfigurationBuilder.matchYarnQueue(matchYarnQueue);
            return this;
        }

        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            if (CollectionUtils.isNotEmpty(entities)) {
                if (entities.contains(BusinessEntity.Account)) {
                    processAccountStepConfiguration.setRebuild(true);
                }
                if (entities.contains(BusinessEntity.Contact)) {
                    processContactStepConfiguration.setRebuild(true);
                }
                if (entities.contains(BusinessEntity.Product)) {
                    processProductStepConfiguration.setRebuild(true);
                }
                if (entities.contains(BusinessEntity.Transaction)) {
                    processTransactionStepConfiguration.setRebuild(true);
                }
                if (entities.contains(BusinessEntity.Rating)) {
                    processRatingStepConfiguration.setRebuild(true);
                }
            }
            return this;
        }

        public ProcessAnalyzeWorkflowConfiguration build() {
            configuration.add(processStepConfiguration);
            configuration.add(processAccountStepConfiguration);
            configuration.add(processContactStepConfiguration);
            configuration.add(processProductStepConfiguration);
            configuration.add(processTransactionStepConfiguration);
            configuration.add(processRatingStepConfiguration);
            configuration.add(combineStatisticsConfiguration);
            configuration.add(generateRatingWorfklowConfigurationBuilder.build());
            configuration.add(redshiftPublishWorkflowConfigurationBuilder.build());
            configuration.add(awsPythonDataConfiguration);
            return configuration;
        }
    }
}
