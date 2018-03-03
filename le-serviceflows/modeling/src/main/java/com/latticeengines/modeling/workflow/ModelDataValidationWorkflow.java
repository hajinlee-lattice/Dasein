package com.latticeengines.modeling.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.modeling.workflow.steps.CreatePrematchEventTableReport;
import com.latticeengines.modeling.workflow.steps.ValidatePrematchEventTable;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component
@Lazy
public class ModelDataValidationWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Inject
    private CreatePrematchEventTableReport createPrematchEventTableReport;

    @Inject
    private ValidatePrematchEventTable validatePrematchEventTable;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder().next(createPrematchEventTableReport) //
                .next(validatePrematchEventTable) //
                .build();

    }
}
