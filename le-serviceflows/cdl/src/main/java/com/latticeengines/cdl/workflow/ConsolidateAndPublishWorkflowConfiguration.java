package com.latticeengines.cdl.workflow;

import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class ConsolidateAndPublishWorkflowConfiguration extends WorkflowConfiguration {

    private ConsolidateAndPublishWorkflowConfiguration() {
    }

    public static class Builder {

        public ConsolidateAndPublishWorkflowConfiguration configuration = new ConsolidateAndPublishWorkflowConfiguration();
        public RedshiftPublishWorkflowConfiguration.Builder redshiftPublishWorkflowConfigurationBuilder = new RedshiftPublishWorkflowConfiguration.Builder();

        public Builder redshiftTableConfiguration(RedshiftTableConfiguration config) {
            redshiftPublishWorkflowConfigurationBuilder.redshiftTableConfiguration(config);
            return this;
        }

        public ConsolidateAndPublishWorkflowConfiguration build() {
            configuration.add(redshiftPublishWorkflowConfigurationBuilder.build());
            return configuration;
        }
    }

}
