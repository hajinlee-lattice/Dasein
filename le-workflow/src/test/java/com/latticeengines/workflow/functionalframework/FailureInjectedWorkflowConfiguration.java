package com.latticeengines.workflow.functionalframework;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class FailureInjectedWorkflowConfiguration extends WorkflowConfiguration {

    public static class Builder {
        private FailureInjectedWorkflowConfiguration configuration = new FailureInjectedWorkflowConfiguration();
        private DynamicSubWorkflowAConfiguration.Builder subWorkflowA = new DynamicSubWorkflowAConfiguration.Builder();
        private DynamicSubWorkflowBConfiguration.Builder subWorkflowB = new DynamicSubWorkflowBConfiguration.Builder();

        public FailureInjectedWorkflowConfiguration build() {
            configuration.setContainerConfiguration("failureInjectedWorkflow", null,
                    configuration.getClass().getSimpleName());
            configuration.add(subWorkflowA.build());
            configuration.add(subWorkflowB.build());
            return configuration;
        }
    }

}
