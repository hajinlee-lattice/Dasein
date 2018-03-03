package com.latticeengines.workflow.core;

import org.springframework.beans.factory.annotation.Autowired;
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

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder().next(dummyStep) //
                .next(dummyAwsStep) //
                .build();
    }

}
