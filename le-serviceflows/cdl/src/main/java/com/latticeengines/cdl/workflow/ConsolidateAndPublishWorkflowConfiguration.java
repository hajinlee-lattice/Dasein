package com.latticeengines.cdl.workflow;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class ConsolidateAndPublishWorkflowConfiguration extends WorkflowConfiguration {

    private ConsolidateAndPublishWorkflowConfiguration() {
    }

    public static class Builder {

        public ConsolidateAndPublishWorkflowConfiguration configuration = new ConsolidateAndPublishWorkflowConfiguration();
        public RedshiftPublishWorkflowConfiguration.Builder redshiftPublishWorkflowConfigurationBuilder = new RedshiftPublishWorkflowConfiguration.Builder();

        public Builder initialLoad(boolean initialLoad) {
            redshiftPublishWorkflowConfigurationBuilder.initialLoad(initialLoad);
            return this;
        }

        public Builder cleanupS3(boolean cleanupS3) {
            redshiftPublishWorkflowConfigurationBuilder.initialLoad(cleanupS3);
            return this;
        }

        public ConsolidateAndPublishWorkflowConfiguration build() {
            configuration.add(redshiftPublishWorkflowConfigurationBuilder.build());
            return configuration;
        }
    }

}
