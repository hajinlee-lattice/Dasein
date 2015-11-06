package com.latticeengines.workflowapi.flows;

import com.latticeengines.workflow.exposed.build.WorkflowConfiguration;
import com.latticeengines.workflowapi.steps.prospectdiscovery.BaseFitModelStepConfiguration;

public class FitModelWorkflowConfiguration extends WorkflowConfiguration {

    private FitModelWorkflowConfiguration() {
    }

    public static class Builder {

        private FitModelWorkflowConfiguration configuration = new FitModelWorkflowConfiguration();

        public Builder setBaseFitModelStepConfiguration(BaseFitModelStepConfiguration baseFitModelStepConfiguration) {
            configuration.add(baseFitModelStepConfiguration);
            return this;
        }

        public FitModelWorkflowConfiguration build() {
            return configuration;
        }
    }


}
