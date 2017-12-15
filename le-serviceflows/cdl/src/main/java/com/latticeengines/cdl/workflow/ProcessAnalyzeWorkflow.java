package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import com.latticeengines.cdl.workflow.steps.process.AwsApsGeneratorStep;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.choreographers.ProcessAnalyzeChoreographer;
import com.latticeengines.cdl.workflow.listeners.ProcessAnalyzeListener;
import com.latticeengines.cdl.workflow.steps.process.CloneStatistics;
import com.latticeengines.cdl.workflow.steps.process.CombineStatistics;
import com.latticeengines.cdl.workflow.steps.process.FinishProcessing;
import com.latticeengines.cdl.workflow.steps.process.StartProcessing;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processAnalyzeWorkflow")
public class ProcessAnalyzeWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private StartProcessing startProcessing;

    @Inject
    private FinishProcessing finishProcessing;

    @Inject
    private ProcessAnalyzeListener processAnalyzeListener;

    @Inject
    private ProcessAccountWorkflow processAccountWorkflow;

    @Inject
    private ProcessContactWorkflow processContactWorkflow;

    @Inject
    private ProcessProductWorkflow processProductWorkflow;

    @Inject
    private ProcessTransactionWorkflow processTransactionWorkflow;

    @Inject
    private AwsApsGeneratorStep awsApsGeneratorStep;

    @Inject
    private CombineStatistics combineStatistics;

    @Inject
    private CloneStatistics cloneStatistics;

    @Inject
    private RedshiftPublishWorkflow redshiftPublishWorkflow;

    @Inject
    private ProcessAnalyzeChoreographer choreographer;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(startProcessing) //
                .next(processAccountWorkflow) //
                .next(processContactWorkflow) //
                .next(processProductWorkflow) //
                .next(processTransactionWorkflow) //
                .next(combineStatistics) //
                .next(cloneStatistics) //
                .next(redshiftPublishWorkflow) //
                .next(awsApsGeneratorStep) //
                .next(finishProcessing) //
                .listener(processAnalyzeListener) //
                .choreographer(choreographer) //
                .build();
    }

}
