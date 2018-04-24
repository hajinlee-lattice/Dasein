package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.ActivateRatingEngineListener;
import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableFilterStep;
import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.RatingEngineImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.modeling.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.modeling.workflow.steps.DedupEventTable;
import com.latticeengines.scoring.workflow.steps.ExportBucketTool;
import com.latticeengines.scoring.workflow.steps.ExportScoreTrainingFile;
import com.latticeengines.scoring.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.serviceflows.workflow.transformation.AddStandardAttributes;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("ratingEngineImportMatchAndModelWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RatingEngineImportMatchAndModelWorkflow
        extends AbstractWorkflow<RatingEngineImportMatchAndModelWorkflowConfiguration> {

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
    private CdlModelWorkflow modelWorkflow;

    @Inject
    private SetConfigurationForScoring setConfigurationForScoring;

    @Inject
    private GenerateAIRatingWorkflow generateRating;

    @Inject
    private ExportBucketTool exportBucketTool;

    @Inject
    private ExportScoreTrainingFile exportScoreTrainingFile;

    @Inject
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Inject
    private ActivateRatingEngineListener activateRatingEngineListener;

    @Override
    public Workflow defineWorkflow(RatingEngineImportMatchAndModelWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(createCdlEventTableFilterStep) //
                .next(createCdlEventTableStep) //
                .next(matchDataCloudWorkflow) //
                .next(dedupEventTableDataFlow) //
                .next(addStandardAttributesDataFlow) //
                .next(modelWorkflow) //
                .next(setConfigurationForScoring) //
                .next(generateRating) //
                .next(exportScoreTrainingFile) //
                .next(exportBucketTool) //
                .listener(activateRatingEngineListener) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }
}
