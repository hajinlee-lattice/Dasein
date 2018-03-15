package com.latticeengines.workflow.functionalframework;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("failureInjectedWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FailureInjectedWorkflow extends AbstractWorkflow<FailureInjectedWorkflowConfiguration> {

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
    private InjectedFailureListener injectedFailureListener;

    @Override
    public Workflow defineWorkflow(FailureInjectedWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(stepA) //
                .next(stepB) //
                .next(subWorkflowA) //
                .next(stepC) //
                .next(subWorkflowB) //
                .next(stepD) //
                .listener(injectedFailureListener) //
                .build();
    }

}
