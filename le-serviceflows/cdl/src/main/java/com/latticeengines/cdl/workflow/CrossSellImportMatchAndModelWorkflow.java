package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableFilterStep;
import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.CrossSellImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.modeling.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.modeling.workflow.steps.DedupEventTable;
import com.latticeengines.modeling.workflow.steps.MergeUserRefinedAttributes;
import com.latticeengines.modeling.workflow.steps.UseConfiguredModelingAttributes;
import com.latticeengines.scoring.workflow.steps.ExportBucketTool;
import com.latticeengines.scoring.workflow.steps.ExportScoreTrainingFile;
import com.latticeengines.scoring.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.serviceflows.workflow.export.ExportModelToS3;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.serviceflows.workflow.transformation.AddStandardAttributes;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("crossSellImportMatchAndModelWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CrossSellImportMatchAndModelWorkflow
        extends AbstractWorkflow<CrossSellImportMatchAndModelWorkflowConfiguration> {

    @Inject
    private CreateCdlEventTableFilterStep createCdlEventTableFilterStep;

    @Inject
    private CreateCdlEventTableStep createCdlEventTableStep;

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Inject
    private DedupEventTable dedupEventTableDataFlow;

    @Inject
    private AddStandardAttributes addStandardAttributesDataFlow;

    @Inject
    private UseConfiguredModelingAttributes useConfiguredModelingAttributes;

    @Inject
    private MergeUserRefinedAttributes mergeUserRefinedAttributes;

    @Inject
    private CdlModelWorkflow modelWorkflow;

    @Inject
    private SetConfigurationForScoring setConfigurationForScoring;

    @Inject
    private GenerateAIRatingWorkflow generateRating;

    @Inject
    private ExportBucketTool exportBucketTool;

    @Inject
    private ExportModelToS3 modelExportToS3;

    @Inject
    private ExportScoreTrainingFile exportScoreTrainingFile;

    @Inject
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Override
    public Workflow defineWorkflow(CrossSellImportMatchAndModelWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(createCdlEventTableFilterStep) //
                .next(createCdlEventTableStep) //
                .next(matchDataCloudWorkflow) //
                .next(dedupEventTableDataFlow) //
                .next(addStandardAttributesDataFlow) //
                .next(useConfiguredModelingAttributes) //
                .next(mergeUserRefinedAttributes) //
                .next(modelWorkflow) //
                .next(setConfigurationForScoring) //
                .next(generateRating) //
                .next(exportScoreTrainingFile) //
                .next(exportBucketTool) //
                .next(modelExportToS3) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }
}
