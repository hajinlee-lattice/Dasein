package com.latticeengines.workflow.functionalframework;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("sleepableWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SleepableWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Inject
    private SuccessfulStep successfulStep;

    @Inject
    private AnotherSuccessfulStep anotherSuccessfulStep;

    @Inject
    private SleepableStep sleepableStep;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(successfulStep) //
                .next(sleepableStep) //
                .next(anotherSuccessfulStep) //
                .build();
    }

}
