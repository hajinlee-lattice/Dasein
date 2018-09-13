package com.latticeengines.workflow.functionalframework;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class DynamicSubWorkflowAConfiguration extends WorkflowConfiguration {

    public static class Builder {
        private DynamicSubWorkflowAConfiguration configuration = new DynamicSubWorkflowAConfiguration();
        private DynamicSubWorkflowBConfiguration.Builder subWorkflowB = new DynamicSubWorkflowBConfiguration.Builder();

        public DynamicSubWorkflowAConfiguration build() {
            configuration.setContainerConfiguration("dynamicSubWorkflowA", null,
                    configuration.getClass().getSimpleName());
            configuration.add(subWorkflowB.build());
            return configuration;
        }
    }

}
