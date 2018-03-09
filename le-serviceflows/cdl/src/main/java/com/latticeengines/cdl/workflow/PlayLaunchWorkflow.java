package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.PlayLaunchInitStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.PlayLaunchWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("playLaunchWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PlayLaunchWorkflow extends AbstractWorkflow<PlayLaunchWorkflowConfiguration> {

    @Inject
    private PlayLaunchInitStep playLaunchInitStep;

    @Override
    public Workflow defineWorkflow(PlayLaunchWorkflowConfiguration config) {
        return new WorkflowBuilder(name()) //
                .next(playLaunchInitStep) //
                .build();
    }
}
