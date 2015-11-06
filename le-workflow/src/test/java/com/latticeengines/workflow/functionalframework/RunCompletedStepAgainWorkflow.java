package com.latticeengines.workflow.functionalframework;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;
import com.latticeengines.workflow.exposed.build.WorkflowConfiguration;

@Component("runCompletedStepAgainWorkflow")
public class RunCompletedStepAgainWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private FailableWorkflow failableWorkflow;

    @Autowired
    private RunAgainWhenCompleteStep runAgainWhenCompleteStep;

    @Bean
    public Job runCompletedStepAgainWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(runAgainWhenCompleteStep) //
                .next(failableWorkflow) //
                .build();
    }

}
