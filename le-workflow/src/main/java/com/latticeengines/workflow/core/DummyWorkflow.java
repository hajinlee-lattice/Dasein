package com.latticeengines.workflow.core;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("dummyWorkflow")
public class DummyWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private DummyStep dummyStep;

    @Autowired
    private DummyAwsStep dummyAwsStep;

    @Bean
    public Job dummyWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(dummyStep) //
                .next(dummyAwsStep) //
                .build();
    }

}
