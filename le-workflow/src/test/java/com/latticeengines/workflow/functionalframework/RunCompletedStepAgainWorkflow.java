package com.latticeengines.workflow.functionalframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("runCompletedStepAgainWorkflow")
public class RunCompletedStepAgainWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private FailableWorkflow failableWorkflow;

    @Autowired
    private RunAgainWhenCompleteStep runAgainWhenCompleteStep;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder().next(runAgainWhenCompleteStep) //
                .next(failableWorkflow, null) //
                .build();
    }

}
