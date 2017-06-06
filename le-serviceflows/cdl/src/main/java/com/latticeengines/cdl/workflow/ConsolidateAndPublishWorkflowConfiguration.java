package com.latticeengines.cdl.workflow;

import java.util.List;
import java.util.Map;

import com.latticeengines.cdl.workflow.steps.ConsolidateDataConfiguration;
import com.latticeengines.cdl.workflow.steps.StartExecutionConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class ConsolidateAndPublishWorkflowConfiguration extends WorkflowConfiguration {

    private ConsolidateAndPublishWorkflowConfiguration() {
    }

    public static class Builder {

        public ConsolidateAndPublishWorkflowConfiguration configuration = new ConsolidateAndPublishWorkflowConfiguration();
        public StartExecutionConfiguration startExecutionConfiguration = new StartExecutionConfiguration();
        public ConsolidateDataConfiguration consolidateDataConfiguration = new ConsolidateDataConfiguration();

        public RedshiftPublishWorkflowConfiguration.Builder redshiftPublishWorkflowConfigurationBuilder = new RedshiftPublishWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("consolidateAndPublishWorkflow", customerSpace,
                    "consolidateAndPublishWorkflow");
            startExecutionConfiguration.setCustomerSpace(customerSpace);
            redshiftPublishWorkflowConfigurationBuilder.customer(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            startExecutionConfiguration.setMicroServiceHostPort(microServiceHostPort);
            redshiftPublishWorkflowConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder datafeedName(String datafeedName) {
            startExecutionConfiguration.setDataFeedName(datafeedName);
            return this;
        }

        public Builder hdfsToRedshiftConfiguration(HdfsToRedshiftConfiguration hdfsToRedshiftConfiguration) {
            redshiftPublishWorkflowConfigurationBuilder.hdfsToRedshiftConfiguration(hdfsToRedshiftConfiguration);
            return this;
        }

        public Builder masterTableName(String masterTableName) {
            consolidateDataConfiguration.setMasterTableName(masterTableName);
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

        public ConsolidateAndPublishWorkflowConfiguration build() {
            configuration.add(startExecutionConfiguration);
            configuration.add(consolidateDataConfiguration);
            configuration.add(redshiftPublishWorkflowConfigurationBuilder.build());
            return configuration;
        }
    }

}
