package com.latticeengines.workflowapi.flows;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflowapi.steps.dlorchestration.ModelLoadDataConfiguration;

public class DLOrchestrationWorkflowConfiguration extends WorkflowConfiguration {

    private DLOrchestrationWorkflowConfiguration() {
    }

    public static class Builder {

        private DLOrchestrationWorkflowConfiguration configuration = new DLOrchestrationWorkflowConfiguration();

        public Builder setModelLoadDataConfiguration(ModelLoadDataConfiguration modelLoadDataConfiguration) {
            configuration.add(modelLoadDataConfiguration);
            return this;
        }

        public DLOrchestrationWorkflowConfiguration build() {
            return configuration;
        }
    }

}
