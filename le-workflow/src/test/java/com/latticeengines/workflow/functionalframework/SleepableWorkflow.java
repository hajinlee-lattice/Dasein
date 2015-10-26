package com.latticeengines.workflow.functionalframework;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.build.AbstractWorkflow;
import com.latticeengines.workflow.build.WorkflowBuilder;
import com.latticeengines.workflow.core.Workflow;

@Component("sleepableWorkflow")
public class SleepableWorkflow extends AbstractWorkflow {

    @Autowired
    private SuccessfulStep successfulStep;

    @Autowired
    private AnotherSuccessfulStep anotherSuccessfulStep;

    @Autowired
    private SleepableStep sleepableStep;

    @Bean
    public Job buildSleepableWorkflow() throws Exception {
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
