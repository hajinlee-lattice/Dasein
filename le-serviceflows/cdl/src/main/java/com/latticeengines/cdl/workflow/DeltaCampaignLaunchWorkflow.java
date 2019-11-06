package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.DeltaCampaignLaunchWorkflowListener;
import com.latticeengines.cdl.workflow.steps.DeltaCampaignLaunchInitStep;
import com.latticeengines.cdl.workflow.steps.play.DeltaCampaignLaunchExportFileGeneratorStep;
import com.latticeengines.cdl.workflow.steps.play.DeltaCampaignLaunchExportFilesToS3Step;
import com.latticeengines.cdl.workflow.steps.play.DeltaCampaignLaunchExportPublishToSNSStep;
import com.latticeengines.cdl.workflow.steps.play.ImportDeltaCalculationResultsFromS3;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("deltaCampaignLaunchWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DeltaCampaignLaunchWorkflow extends AbstractWorkflow<DeltaCampaignLaunchWorkflowConfiguration> {

    @Inject
    private ImportDeltaCalculationResultsFromS3 importDeltaCalculationResultsFromS3;

    @Inject
    private DeltaCampaignLaunchInitStep deltaCampaignLaunchInitStep;

    @Inject
    private DeltaCampaignLaunchExportFileGeneratorStep deltaCampaignLaunchExportFileGeneratorStep;

    @Inject
    private DeltaCampaignLaunchExportFilesToS3Step deltaCampaignLaunchExportFilesToS3Step;

    @Inject
    private DeltaCampaignLaunchExportPublishToSNSStep deltaCampaignLaunchExportPublishToSNSStep;

    @Inject
    private DeltaCampaignLaunchWorkflowListener deltaCampaignLaunchWorkflowListener;

    @Override
    public Workflow defineWorkflow(DeltaCampaignLaunchWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(importDeltaCalculationResultsFromS3) //
                .next(deltaCampaignLaunchInitStep) //
                .next(deltaCampaignLaunchExportFileGeneratorStep) //
                .next(deltaCampaignLaunchExportFilesToS3Step) //
                .next(deltaCampaignLaunchExportPublishToSNSStep) //
                .listener(deltaCampaignLaunchWorkflowListener) //
                .build();
    }
}
