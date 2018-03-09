package com.latticeengines.workflow.functionalframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("failableWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FailableWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private SuccessfulStep successfulStep;

    @Autowired
    private AnotherSuccessfulStep anotherSuccessfulStep;

    @Autowired
    private FailableStep failableStep;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder(name()) //
                .next(successfulStep) //
                .next(failableStep) //
                .next(anotherSuccessfulStep) //
                .build();
    }

}
