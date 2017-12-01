package com.latticeengines.workflow.functionalframework;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.batch.core.Job;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("dynamicSubWorkflowA")
public class DynamicSubWorkflowA extends AbstractWorkflow<WorkflowConfiguration> {

    @Resource(name = "stepA")
    private NamedStep stepA;

    @Resource(name = "stepB")
    private NamedStep stepB;

    @Inject
    private DynamicSubWorkflowB subWorkflowB;

    @Bean
    public Job dynamicSubWorkflowAJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(stepA) //
                .next(stepB) //
                .next(subWorkflowB) //
                .build();
    }

}
