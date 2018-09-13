package com.latticeengines.workflow.functionalframework;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class DynamicSubWorkflowBConfiguration extends WorkflowConfiguration {

    public static class Builder {
        private DynamicSubWorkflowBConfiguration configuration = new DynamicSubWorkflowBConfiguration();

        public DynamicSubWorkflowBConfiguration build() {
            configuration.setContainerConfiguration("dynamicSubWorkflowB", null,
                    configuration.getClass().getSimpleName());
            return configuration;
        }
    }

}
