package com.latticeengines.workflow.functionalframework;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;
import com.latticeengines.workflow.exposed.build.WorkflowConfiguration;

@Component("sleepableWorkflow")
public class SleepableWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private SuccessfulStep successfulStep;

    @Autowired
    private AnotherSuccessfulStep anotherSuccessfulStep;

    @Autowired
    private SleepableStep sleepableStep;

    @Bean
    public Job sleepableWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(successfulStep) //
                .next(sleepableStep) //
                .next(anotherSuccessfulStep) //
                .build();
    }

}
