package com.latticeengines.workflow.functionalframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("workflowWithFailingListener")
public class WorkflowWithFailingListener extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private SuccessfulStep successfulStep;

    @Autowired
    private FailingListener failingListener;

    @Autowired
    private SuccessfulListener successfulListener;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder().next(successfulStep) //
                .listener(failingListener) //
                .listener(successfulListener).build();
    }
}