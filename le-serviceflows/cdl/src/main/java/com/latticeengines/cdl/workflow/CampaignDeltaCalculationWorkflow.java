package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.CampaignDeltaCalculationWorkflowListener;
import com.latticeengines.cdl.workflow.steps.campaign.CalculateDeltaStep;
import com.latticeengines.cdl.workflow.steps.campaign.ExportDeltaArtifactsToS3;
import com.latticeengines.cdl.workflow.steps.campaign.GenerateLaunchArtifacts;
import com.latticeengines.cdl.workflow.steps.campaign.GenerateLaunchUniverse;
import com.latticeengines.cdl.workflow.steps.campaign.ImportDeltaArtifactsFromS3;
import com.latticeengines.cdl.workflow.steps.campaign.QueuePlayLaunches;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.CampaignDeltaCalculationWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("campaignDeltaCalculationWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CampaignDeltaCalculationWorkflow extends AbstractWorkflow<CampaignDeltaCalculationWorkflowConfiguration> {

    @Inject
    private ImportDeltaArtifactsFromS3 importDeltaArtifactsFromS3;

    @Inject
    private GenerateLaunchUniverse generateLaunchUniverse;

    @Inject
    private CalculateDeltaStep calculateDeltaStep;

    @Inject
    private GenerateLaunchArtifacts generateLaunchArtifacts;

    @Inject
    private ExportDeltaArtifactsToS3 exportArtifactsToS3Step;

    @Inject
    private QueuePlayLaunches queuePlayLaunches;

    @Inject
    private CampaignDeltaCalculationWorkflowListener campaignDeltaCalculationWorkflowListener;

    @Override
    public Workflow defineWorkflow(CampaignDeltaCalculationWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(importDeltaArtifactsFromS3) //
                .next(generateLaunchUniverse) //
                .next(calculateDeltaStep) //
                .next(generateLaunchArtifacts) //
                .next(exportArtifactsToS3Step) //
                .next(queuePlayLaunches) //
                .listener(campaignDeltaCalculationWorkflowListener) //
                .build();
    }
}
