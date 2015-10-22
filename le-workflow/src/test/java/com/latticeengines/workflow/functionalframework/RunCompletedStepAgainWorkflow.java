package com.latticeengines.workflow.functionalframework;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.build.AbstractWorkflow;
import com.latticeengines.workflow.build.WorkflowBuilder;
import com.latticeengines.workflow.core.Workflow;

@Component("RunCompletedStepAgainWorkflow")
public class RunCompletedStepAgainWorkflow extends AbstractWorkflow {

    @Autowired
    private FailableWorkflow failableWorkflow;

    @Autowired
    private RunAgainWhenCompleteStep runAgainWhenCompleteStep;

    @Bean
    public Job buildRunCompletedStepAgainWorkflow() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(runAgainWhenCompleteStep) //
                .next(failableWorkflow) //
                .build();
    }

}
