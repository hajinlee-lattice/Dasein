package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.campaign.QueuePlayLaunchesStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.CampaignDeltaCalculationWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("campaignDeltaCalculationWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CampaignDeltaCalculationWorkflow extends AbstractWorkflow<CampaignDeltaCalculationWorkflowConfiguration> {

    @Inject
    private QueuePlayLaunchesStep queuePlayLaunchesStep;

    @Override
    public Workflow defineWorkflow(CampaignDeltaCalculationWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(queuePlayLaunchesStep) //
                .build();
    }
}
