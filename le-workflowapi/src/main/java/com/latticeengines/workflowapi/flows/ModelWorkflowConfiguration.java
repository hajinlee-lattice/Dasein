package com.latticeengines.workflowapi.flows;

import com.latticeengines.workflow.exposed.build.WorkflowConfiguration;
import com.latticeengines.workflowapi.steps.dlorchestration.ModelLoadDataConfiguration;

public class ModelWorkflowConfiguration extends WorkflowConfiguration {

    private ModelWorkflowConfiguration() {
    }

    public static class Builder {

        private ModelWorkflowConfiguration configuration = new ModelWorkflowConfiguration();

        public Builder setModelLoadDataConfiguration(ModelLoadDataConfiguration modelLoadDataConfiguration) {
            configuration.add(modelLoadDataConfiguration);
            return this;
        }

        public ModelWorkflowConfiguration build() {
            return configuration;
        }
    }

}
