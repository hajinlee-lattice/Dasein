package com.latticeengines.workflow.core;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("dummyWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DummyWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Inject
    private DummyStep dummyStep;

    @Inject
    private DummyAwsStep dummyAwsStep;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(dummyStep) //
                .next(dummyAwsStep) //
                .build();
    }

}
