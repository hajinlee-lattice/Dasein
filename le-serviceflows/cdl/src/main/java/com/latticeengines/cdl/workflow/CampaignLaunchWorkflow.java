package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.CampaignLaunchWorkflowListener;
import com.latticeengines.cdl.workflow.steps.CampaignLaunchInitStep;
import com.latticeengines.cdl.workflow.steps.export.ImportExtractEntityFromS3;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchExportFileGeneratorStep;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchExportFilesToS3Step;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchExportPublishToSNSStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.CampaignLaunchWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("campaignLaunchWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CampaignLaunchWorkflow extends AbstractWorkflow<CampaignLaunchWorkflowConfiguration> {

    @Inject
    private ImportExtractEntityFromS3 importExtractEntityFromS3;

    @Inject
    private CampaignLaunchInitStep campaignLaunchInitStep;

    @Inject
    private CampaignLaunchExportFileGeneratorStep campaignLaunchExportFileGeneratorStep;

    @Inject
    private CampaignLaunchExportFilesToS3Step campaignLaunchExportFilesToS3Step;

    @Inject
    private CampaignLaunchExportPublishToSNSStep campaignLaunchExportPublishToSNSStep;

    @Inject
    private CampaignLaunchWorkflowListener campaignLaunchWorkflowListener;

    @Override
    public Workflow defineWorkflow(CampaignLaunchWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(importExtractEntityFromS3) //
                .next(campaignLaunchInitStep) //
                .next(campaignLaunchExportFileGeneratorStep) //
                .next(campaignLaunchExportFilesToS3Step) //
                .next(campaignLaunchExportPublishToSNSStep) //
                .listener(campaignLaunchWorkflowListener) //
                .build();
    }
}
