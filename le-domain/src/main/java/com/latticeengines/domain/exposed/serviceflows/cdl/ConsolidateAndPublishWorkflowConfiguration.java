package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateDataConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.StartExecutionConfiguration;

public class ConsolidateAndPublishWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    private ConsolidateAndPublishWorkflowConfiguration() {
    }

    public static class Builder {

        public ConsolidateAndPublishWorkflowConfiguration configuration = new ConsolidateAndPublishWorkflowConfiguration();
        public StartExecutionConfiguration startExecutionConfiguration = new StartExecutionConfiguration();
        public ConsolidateDataConfiguration consolidateDataConfiguration = new ConsolidateDataConfiguration();

        public RedshiftPublishWorkflowConfiguration.Builder redshiftPublishWorkflowConfigurationBuilder = new RedshiftPublishWorkflowConfiguration.Builder();

        public Builder initialDataFeedStatus(Status initialDataFeedStatus) {
            startExecutionConfiguration.setInitialDataFeedStatus(initialDataFeedStatus);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("consolidateAndPublishWorkflow", customerSpace,
                    "consolidateAndPublishWorkflow");
            startExecutionConfiguration.setCustomerSpace(customerSpace);
            consolidateDataConfiguration.setCustomerSpace(customerSpace);
            redshiftPublishWorkflowConfigurationBuilder.customer(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            startExecutionConfiguration.setMicroServiceHostPort(microServiceHostPort);
            redshiftPublishWorkflowConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder hdfsToRedshiftConfiguration(HdfsToRedshiftConfiguration hdfsToRedshiftConfiguration) {
            redshiftPublishWorkflowConfigurationBuilder.hdfsToRedshiftConfiguration(hdfsToRedshiftConfiguration);
            return this;
        }

        public Builder dataCollectionName(String dataCollectionName) {
            consolidateDataConfiguration.setDataCollectionName(dataCollectionName);
            return this;
        }

        public Builder idField(String idField) {
            consolidateDataConfiguration.setIdField(idField);
            return this;
        }

        public Builder matchKeyMap(Map<MatchKey, List<String>> matchKeyMap) {
            consolidateDataConfiguration.setMatchKeyMap(matchKeyMap);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder dropConsolidatedTable(Boolean drop) {
            redshiftPublishWorkflowConfigurationBuilder.dropSourceTable(drop);
            return this;
        }

        public ConsolidateAndPublishWorkflowConfiguration build() {
            configuration.add(startExecutionConfiguration);
            configuration.add(consolidateDataConfiguration);
            configuration.add(redshiftPublishWorkflowConfigurationBuilder.build());
            return configuration;
        }

    }

}
