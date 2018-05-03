package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.choreographers.ProcessAnalyzeChoreographer;
import com.latticeengines.cdl.workflow.listeners.ProcessAnalyzeListener;
import com.latticeengines.serviceflows.workflow.export.ExportToRedshift;
import com.latticeengines.cdl.workflow.steps.process.AwsApsGeneratorStep;
import com.latticeengines.cdl.workflow.steps.process.CombineStatistics;
import com.latticeengines.cdl.workflow.steps.process.FinishProcessing;
import com.latticeengines.cdl.workflow.steps.process.GenerateProcessingReport;
import com.latticeengines.cdl.workflow.steps.process.StartProcessing;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportToDynamo;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processAnalyzeWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessAnalyzeWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private StartProcessing startProcessing;

    @Inject
    private FinishProcessing finishProcessing;

    @Inject
    private GenerateProcessingReport generateProcessingReport;

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
    private ProcessRatingWorkflow processRatingWorkflow;

    @Inject
    private AwsApsGeneratorStep awsApsGeneratorStep;

    @Inject
    private CombineStatistics combineStatistics;

    @Inject
    private ExportToRedshift exportToRedshift;

    @Inject
    private ExportToDynamo exportToDynamo;

    @Inject
    private ProcessAnalyzeChoreographer choreographer;

    @Override
    public Workflow defineWorkflow(ProcessAnalyzeWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(startProcessing) //
                .next(processAccountWorkflow) //
                .next(processContactWorkflow) //
                .next(processProductWorkflow) //
                .next(processTransactionWorkflow) //
                .next(combineStatistics) //
                .next(exportToRedshift) //
                .next(exportToDynamo) //
                .next(awsApsGeneratorStep) //
                .next(processRatingWorkflow) //
                .next(generateProcessingReport) //
                .next(finishProcessing) //
                .listener(processAnalyzeListener) //
                .choreographer(choreographer) //
                .build();
    }

}
