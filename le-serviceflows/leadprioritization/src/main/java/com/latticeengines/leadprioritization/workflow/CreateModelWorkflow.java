package com.latticeengines.leadprioritization.workflow;

import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;

@Component("createModelWorkflow")
public class CreateModelWorkflow extends AbstractWorkflow<CreateModelWorkflowConfiguration> {
    @Override
    public Workflow defineWorkflow() {
        return null;
    }
}
