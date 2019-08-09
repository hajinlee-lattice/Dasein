package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.campaign.CalculateDeltaStep;
import com.latticeengines.cdl.workflow.steps.campaign.QueuePlayLaunchesStep;
import com.latticeengines.cdl.workflow.steps.export.ImportExtractEntityFromS3;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.CampaignDeltaCalculationWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("campaignDeltaCalculationWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CampaignDeltaCalculationWorkflow extends AbstractWorkflow<CampaignDeltaCalculationWorkflowConfiguration> {

    @Inject
    // TODO: Create new one for this workflow
    private ImportExtractEntityFromS3 importFromS3;

    @Inject
    private CalculateDeltaStep calculateDeltaStep;

    @Inject
    private QueuePlayLaunchesStep queuePlayLaunchesStep;

    // TODO: Add step to export files and create metadata tables in the step below

    @Override
    public Workflow defineWorkflow(CampaignDeltaCalculationWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(importFromS3) //
                .next(calculateDeltaStep) //
                .next(queuePlayLaunchesStep) //
                .build();
    }
}
