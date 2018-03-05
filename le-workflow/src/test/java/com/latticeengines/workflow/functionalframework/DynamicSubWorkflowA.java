package com.latticeengines.workflow.functionalframework;

import javax.annotation.Resource;
import javax.inject.Inject;

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

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(stepA) //
                .next(stepB) //
                .next(subWorkflowB, null) //
                .build();
    }

}
