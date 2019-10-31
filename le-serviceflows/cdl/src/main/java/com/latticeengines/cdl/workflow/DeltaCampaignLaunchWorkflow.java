package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.PlayLaunchWorkflowListener;
import com.latticeengines.cdl.workflow.steps.DeltaCampaignLaunchInitStep;
import com.latticeengines.cdl.workflow.steps.export.ImportExtractEntityFromS3;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchExportFileGeneratorStep;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchExportFilesToS3Step;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchExportPublishToSNSStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("deltaCampaignLaunchWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DeltaCampaignLaunchWorkflow extends AbstractWorkflow<DeltaCampaignLaunchWorkflowConfiguration> {

    @Inject
    private ImportExtractEntityFromS3 importExtractEntityFromS3;

    @Inject
    private DeltaCampaignLaunchInitStep deltaCampaignLaunchInitStep;

    @Inject
    private PlayLaunchExportFileGeneratorStep playLaunchExportFileGeneratorStep;

    @Inject
    private PlayLaunchExportFilesToS3Step playLaunchExportFilesToS3Step;

    @Inject
    private PlayLaunchExportPublishToSNSStep playLaunchExportPublishToSNSStep;

    @Inject
    private PlayLaunchWorkflowListener playLaunchWorkflowListener;

    @Override
    public Workflow defineWorkflow(DeltaCampaignLaunchWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(importExtractEntityFromS3) //
                .next(deltaCampaignLaunchInitStep) //
                .next(playLaunchExportFileGeneratorStep) //
                .next(playLaunchExportFilesToS3Step) //
                .next(playLaunchExportPublishToSNSStep) //
                .listener(playLaunchWorkflowListener) //
                .build();
    }
}
