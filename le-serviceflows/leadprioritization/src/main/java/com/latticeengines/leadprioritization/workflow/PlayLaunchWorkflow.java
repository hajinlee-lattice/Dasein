package com.latticeengines.leadprioritization.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.PlayLaunchWorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.PlayLaunchInitStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("playLaunchWorkflow")
public class PlayLaunchWorkflow extends AbstractWorkflow<PlayLaunchWorkflowConfiguration> {

    @Autowired
    private PlayLaunchInitStep playLaunchInitStep;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(playLaunchInitStep) //
                .build();
    }
}
