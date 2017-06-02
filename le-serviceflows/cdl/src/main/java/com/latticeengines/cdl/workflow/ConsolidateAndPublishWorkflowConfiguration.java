package com.latticeengines.cdl.workflow;

import com.latticeengines.cdl.workflow.steps.StartExecutionConfiguration;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class ConsolidateAndPublishWorkflowConfiguration extends WorkflowConfiguration {

    private ConsolidateAndPublishWorkflowConfiguration() {
    }

    public static class Builder {

        public ConsolidateAndPublishWorkflowConfiguration configuration = new ConsolidateAndPublishWorkflowConfiguration();
        public StartExecutionConfiguration startExecutionConfiguration = new StartExecutionConfiguration();
        public RedshiftPublishWorkflowConfiguration.Builder redshiftPublishWorkflowConfigurationBuilder = new RedshiftPublishWorkflowConfiguration.Builder();

        public Builder datafeedName(String datafeedName) {
            startExecutionConfiguration.setDataFeedName(datafeedName);
            return this;
        }

        public Builder hdfsToRedshiftConfiguration(HdfsToRedshiftConfiguration hdfsToRedshiftConfiguration) {
            redshiftPublishWorkflowConfigurationBuilder.hdfsToRedshiftConfiguration(hdfsToRedshiftConfiguration);
            return this;
        }

        public ConsolidateAndPublishWorkflowConfiguration build() {
            configuration.add(startExecutionConfiguration);
            configuration.add(redshiftPublishWorkflowConfigurationBuilder.build());
            return configuration;
        }
    }

}
