package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.PlayLaunchWorkflowListener;
import com.latticeengines.cdl.workflow.steps.CampaignLaunchInitStep;
import com.latticeengines.cdl.workflow.steps.export.ImportExtractEntityFromS3;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchExportFileGeneratorStep;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchExportFilesToS3Step;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchExportPublishToSNSStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.CampaignLaunchWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("campaignLaunchWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CampaignLaunchWorkflow extends AbstractWorkflow<CampaignLaunchWorkflowConfiguration> {

    @Inject
    private ImportExtractEntityFromS3 importFromS3;

    @Inject
    private CampaignLaunchInitStep campaignLaunchInitStep;

    @Inject
    private PlayLaunchExportFileGeneratorStep playLaunchExportFileGeneratorStep;

    @Inject
    private PlayLaunchExportFilesToS3Step playLaunchExportFilesToS3Step;

    @Inject
    private PlayLaunchExportPublishToSNSStep playLaunchExportPublishToSNSStep;

    @Inject
    private PlayLaunchWorkflowListener playLaunchWorkflowListener;

    @Override
    public Workflow defineWorkflow(CampaignLaunchWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(importFromS3) //
                .next(campaignLaunchInitStep) //
                .next(playLaunchExportFileGeneratorStep) //
                .next(playLaunchExportFilesToS3Step) //
                .next(playLaunchExportPublishToSNSStep) //
                .listener(playLaunchWorkflowListener) //
                .build();
    }
}
