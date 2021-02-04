package com.latticeengines.workflow.functionalframework;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("dynamicWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DynamicWorkflow extends AbstractWorkflow<DynamicWorkflowConfiguration> {

    @Resource(name = "stepA")
    private NamedStep stepA;

    @Resource(name = "stepB")
    private NamedStep stepB;

    @Resource(name = "stepC")
    private NamedStep stepC;

    @Resource(name = "stepD")
    private NamedStep stepD;

    @Resource(name = "stepSkippedOnMissingConfig")
    private NamedStep stepSkippedOnMissingConfig;

    @Inject
    private DynamicSubWorkflowA subWorkflowA;

    @Inject
    private DynamicSubWorkflowB subWorkflowB;

    @Inject
    private DynamicWorkflowChoreographer choreographer;

    @Override
    public Workflow defineWorkflow(DynamicWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(stepA) //
                .next(stepB) //
                .next(subWorkflowA) //
                .next(stepC) //
                .next(subWorkflowB) //
                .next(stepD) //
                .next(stepSkippedOnMissingConfig) //
                .choreographer(choreographer) //
                .build();
    }

}
