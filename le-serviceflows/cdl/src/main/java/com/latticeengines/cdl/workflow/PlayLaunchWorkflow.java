package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.CampaignLaunchWorkflowListener;
import com.latticeengines.cdl.workflow.steps.PlayLaunchInitStep;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchExportFileGeneratorStep;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchExportFilesToS3Step;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchExportPublishToSNSStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("playLaunchWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PlayLaunchWorkflow extends AbstractWorkflow<PlayLaunchWorkflowConfiguration> {

    @Inject
    private PlayLaunchInitStep playLaunchInitStep;

    @Inject
    private CampaignLaunchExportFileGeneratorStep campaignLaunchExportFileGeneratorStep;

    @Inject
    private CampaignLaunchExportFilesToS3Step campaignLaunchExportFilesToS3Step;

    @Inject
    private CampaignLaunchExportPublishToSNSStep campaignLaunchExportPublishToSNSStep;

    @Inject
    private CampaignLaunchWorkflowListener campaignLaunchWorkflowListener;

    @Override
    public Workflow defineWorkflow(PlayLaunchWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(playLaunchInitStep) //
                .next(campaignLaunchExportFileGeneratorStep) //
                .next(campaignLaunchExportFilesToS3Step) //
                .next(campaignLaunchExportPublishToSNSStep) //
                .listener(campaignLaunchWorkflowListener) //
                .build();
    }
}
