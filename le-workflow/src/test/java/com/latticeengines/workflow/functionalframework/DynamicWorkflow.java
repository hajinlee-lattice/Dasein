package com.latticeengines.workflow.functionalframework;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("dynamicWorkflow")
public class DynamicWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Resource(name = "stepA")
    private NamedStep stepA;

    @Resource(name = "stepB")
    private NamedStep stepB;

    @Resource(name = "stepC")
    private NamedStep stepC;

    @Resource(name = "stepD")
    private NamedStep stepD;

    @Inject
    private DynamicSubWorkflowA subWorkflowA;

    @Inject
    private DynamicSubWorkflowB subWorkflowB;

    @Inject
    private DynamicWorkflowChoreographer choreographer;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(stepA) //
                .next(stepB) //
                .next(subWorkflowA, null) //
                .next(stepC) //
                .next(subWorkflowB, null) //
                .next(stepD) //
                .choreographer(choreographer) //
                .build();
    }

}
