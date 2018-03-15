package com.latticeengines.workflow.functionalframework;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class DynamicWorkflowConfiguration extends WorkflowConfiguration {

    public static class Builder {
        private DynamicWorkflowConfiguration configuration = new DynamicWorkflowConfiguration();
        private DynamicSubWorkflowAConfiguration.Builder subWorkflowA = new DynamicSubWorkflowAConfiguration.Builder();
        private DynamicSubWorkflowBConfiguration.Builder subWorkflowB = new DynamicSubWorkflowBConfiguration.Builder();

        public DynamicWorkflowConfiguration build() {
            configuration.setContainerConfiguration("dynamicWorkflow", null,
                    configuration.getClass().getSimpleName());
            configuration.add(subWorkflowA.build());
            configuration.add(subWorkflowB.build());
            return configuration;
        }
    }

}
