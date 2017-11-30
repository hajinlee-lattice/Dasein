package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.ProcessAnalyzeListener;
import com.latticeengines.cdl.workflow.steps.AwsApsGeneratorStep;
import com.latticeengines.cdl.workflow.steps.FinishProfile;
import com.latticeengines.cdl.workflow.steps.StartProcessing;
import com.latticeengines.cdl.workflow.steps.UpdateStatsObjects;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processAnalyzeWorkflow")
public class ProcessAnalyzeWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private StartProcessing startProcessing;

    @Inject
    private ProcessAccountWorkflow processAccountWorkflow;

    @Inject
    private ProcessContactWorkflow processContactWorkflow;

    @Inject
    private ProcessProductWorkflow processProductWorkflow;

    @Inject
    private ProcessTransactionWorkflow processTransactionWorkflow;

    @Inject
    private UpdateStatsObjects updateStatsObjects;

    @Inject
    private RedshiftPublishWorkflow redshiftPublishWorkflow;

    @Inject
    private FinishProfile finishProfile;

    @Autowired
    private AwsApsGeneratorStep apsGenerator;

    @Inject
    private ProcessAnalyzeListener processAnalyzeListener;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(startProcessing) //
                .next(processAccountWorkflow) //
                .next(processContactWorkflow)//
                .next(processProductWorkflow)//
                .next(processTransactionWorkflow) //
                .next(apsGenerator)//
                .next(updateStatsObjects) //
                .next(redshiftPublishWorkflow) //
                .next(updateStatsObjects) //
                .next(redshiftPublishWorkflow) //
                .next(finishProfile) //
                .listener(processAnalyzeListener) //
                .build();
    }

}
