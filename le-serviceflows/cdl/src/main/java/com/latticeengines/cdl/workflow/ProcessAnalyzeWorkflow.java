package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.choreographers.ProcessAnalyzeChoreographer;
import com.latticeengines.cdl.workflow.listeners.ProcessAnalyzeListener;
import com.latticeengines.cdl.workflow.steps.process.ApsGeneration;
import com.latticeengines.cdl.workflow.steps.process.AwsApsGeneratorStep;
import com.latticeengines.cdl.workflow.steps.process.CombineStatistics;
import com.latticeengines.cdl.workflow.steps.process.FinishProcessing;
import com.latticeengines.cdl.workflow.steps.process.GenerateProcessingReport;
import com.latticeengines.cdl.workflow.steps.process.StartProcessing;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportProcessAnalyzeToS3;
import com.latticeengines.serviceflows.workflow.export.ExportToDynamo;
import com.latticeengines.serviceflows.workflow.export.ExportToRedshift;
import com.latticeengines.serviceflows.workflow.export.ImportProcessAnalyzeFromS3;
import com.latticeengines.serviceflows.workflow.match.CommitEntityMatchWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component(ProcessAnalyzeWorkflowConfiguration.WORKFLOW_NAME)
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
    private MatchEntityWorkflow matchEntityWorkflow;

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
    private ApsGeneration apsGeneration;

    @Inject
    private CuratedAttributesWorkflow curatedAttributesWorkflow;

    @Inject
    private CombineStatistics combineStatistics;

    @Inject
    private ExportToRedshift exportToRedshift;

    @Inject
    private ExportToDynamo exportToDynamo;

    @Inject
    private ImportProcessAnalyzeFromS3 importProcessAnalyzeFromS3;

    @Inject
    private ExportProcessAnalyzeToS3 exportProcessAnalyzeToS3;

    @Inject
    private ProcessAnalyzeChoreographer choreographer;

    @Inject
    private CommitEntityMatchWorkflow commitEntityMatchWorkflow;

    @Value("${cdl.aps.use.spark}")
    private boolean apsUseSpark;

    @Override
    public Workflow defineWorkflow(ProcessAnalyzeWorkflowConfiguration config) {
        AbstractStep<? extends BaseStepConfiguration> apsStep = apsUseSpark ? apsGeneration : awsApsGeneratorStep;
        return new WorkflowBuilder(name(), config) //
                .next(startProcessing) //
                .next(importProcessAnalyzeFromS3) //
                .next(matchEntityWorkflow) //
                .next(processAccountWorkflow) //
                .next(processContactWorkflow) //
                .next(processProductWorkflow) //
                .next(processTransactionWorkflow) //
                .next(apsStep) //
                .next(curatedAttributesWorkflow) //
                .next(combineStatistics) //
                .next(exportToRedshift) //
                .next(exportToDynamo) //
                .next(processRatingWorkflow) //
                .next(generateProcessingReport) //
                .next(exportProcessAnalyzeToS3) //
                .next(commitEntityMatchWorkflow) //
                .next(finishProcessing) //
                .listener(processAnalyzeListener) //
                .choreographer(choreographer) //
                .build();
    }

}
