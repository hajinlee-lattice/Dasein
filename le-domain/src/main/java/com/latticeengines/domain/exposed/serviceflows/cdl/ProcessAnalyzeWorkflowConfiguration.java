package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.period.PeriodStrategy;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.StartProcessingConfiguration;

public class ProcessAnalyzeWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    private ProcessAnalyzeWorkflowConfiguration() {

    }

    public static class Builder {
        public ProcessAnalyzeWorkflowConfiguration configuration = new ProcessAnalyzeWorkflowConfiguration();
        public StartProcessingConfiguration startProcessingConfiguration = new StartProcessingConfiguration();
        public ConsolidateDataWorkflowConfiguration.Builder consolidateDataConfigurationBuilder = new ConsolidateDataWorkflowConfiguration.Builder();
        public RedshiftPublishWorkflowConfiguration.Builder redshiftPublishWorkflowConfigurationBuilder = new RedshiftPublishWorkflowConfiguration.Builder();

        public Builder initialDataFeedStatus(Status initialDataFeedStatus) {
            startProcessingConfiguration.setInitialDataFeedStatus(initialDataFeedStatus);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("consolidateAndPublishWorkflow", customerSpace,
                    "consolidateAndPublishWorkflow");
            startProcessingConfiguration.setCustomerSpace(customerSpace);
            consolidateDataConfigurationBuilder.customer(customerSpace);
            redshiftPublishWorkflowConfigurationBuilder.customer(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            startProcessingConfiguration.setMicroServiceHostPort(microServiceHostPort);
            redshiftPublishWorkflowConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            startProcessingConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            consolidateDataConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            redshiftPublishWorkflowConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder hdfsToRedshiftConfiguration(HdfsToRedshiftConfiguration hdfsToRedshiftConfiguration) {
            redshiftPublishWorkflowConfigurationBuilder.hdfsToRedshiftConfiguration(hdfsToRedshiftConfiguration);
            return this;
        }

        public Builder accountIdField(String idField) {
            consolidateDataConfigurationBuilder.accountIdField(idField);
            return this;
        }

        public Builder contactIdField(String idField) {
            consolidateDataConfigurationBuilder.contactIdField(idField);
            return this;
        }

        public Builder productIdField(String idField) {
            consolidateDataConfigurationBuilder.productIdField(idField);
            return this;
        }

        public Builder transactionIdField(String idField) {
            consolidateDataConfigurationBuilder.transactionIdField(idField);
            return this;
        }

        public Builder periodStrategy(PeriodStrategy periodStrategy) {
            consolidateDataConfigurationBuilder.periodStrategy(periodStrategy);
            return this;
        }

        public Builder bucketAccount(boolean bucketAccount) {
            consolidateDataConfigurationBuilder.bucketAccount(bucketAccount);
            return this;
        }

        public Builder bucketContact(boolean bucketContact) {
            consolidateDataConfigurationBuilder.bucketContact(bucketContact);
            return this;
        }

        public Builder bucketTransaction(boolean bucketTransaction) {
            consolidateDataConfigurationBuilder.bucketTransaction(bucketTransaction);
            return this;
        }

        public Builder bucketProduct(boolean bucketProduct) {
            consolidateDataConfigurationBuilder.bucketProduct(bucketProduct);
            return this;
        }

        public Builder matchKeyMap(Map<MatchKey, List<String>> matchKeyMap) {
            consolidateDataConfigurationBuilder.matchKeyMap(matchKeyMap);
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

        public Builder importJobIds(List<Long> importJobIds) {
            startProcessingConfiguration.setImportJobIds(importJobIds);
            return this;
        }

        public ProcessAnalyzeWorkflowConfiguration build() {
            configuration.add(startProcessingConfiguration);
            configuration.add(consolidateDataConfigurationBuilder.build());
            configuration.add(redshiftPublishWorkflowConfigurationBuilder.build());
            return configuration;
        }
    }
}
