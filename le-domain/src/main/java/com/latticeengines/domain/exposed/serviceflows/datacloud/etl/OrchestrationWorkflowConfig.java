package com.latticeengines.domain.exposed.serviceflows.datacloud.etl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.EngineConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.OrchestrationConfig;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.OrchestrationStepConfig;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class OrchestrationWorkflowConfig extends WorkflowConfiguration {
    public static class Builder {
        private OrchestrationWorkflowConfig orchWorkflowConfig = new OrchestrationWorkflowConfig();
        private OrchestrationStepConfig orchStepConfig = new OrchestrationStepConfig();

        public Builder orchestrationProgress(OrchestrationProgress progress) {
            orchStepConfig.setOrchestrationProgress(progress);
            return this;
        }

        public Builder orchestration(Orchestration orch) {
            orchStepConfig.setOrchestration(orch);
            return this;
        }

        public Builder orchestrationConfig(OrchestrationConfig orchConfig) {
            orchStepConfig.setOrchestrationConfig(orchConfig);
            return this;
        }

        public OrchestrationWorkflowConfig build() {
            orchWorkflowConfig.setContainerConfiguration("orchestrationWorkflow", EngineConstants.PRODATA_CUSTOMERSPACE,
                    "OrchestrationWorkflow");
            orchWorkflowConfig.setCustomerSpace(CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
            orchWorkflowConfig.add(orchStepConfig);
            return orchWorkflowConfig;
        }
    }
}
