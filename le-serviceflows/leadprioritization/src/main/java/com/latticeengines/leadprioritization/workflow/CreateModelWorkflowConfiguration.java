package com.latticeengines.leadprioritization.workflow;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;

public class CreateModelWorkflowConfiguration extends WorkflowConfiguration {

    private CreateModelWorkflowConfiguration() {
    }

    public static class Builder {
        private CreateModelWorkflowConfiguration configuration = new CreateModelWorkflowConfiguration();
        private ModelStepConfiguration profileAndModel = new ModelStepConfiguration();

    }

}
