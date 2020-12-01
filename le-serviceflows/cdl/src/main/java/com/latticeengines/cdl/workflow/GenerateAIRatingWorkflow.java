package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableStep;
import com.latticeengines.cdl.workflow.steps.ScoreAggregateFlow;
import com.latticeengines.cdl.workflow.steps.rating.ExtractScoringTarget;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.GenerateAIRatingWorkflowConfiguration;
import com.latticeengines.scoring.workflow.steps.CalculateExpectedRevenuePercentileDataFlow;
import com.latticeengines.scoring.workflow.steps.CalculateExpectedRevenuePercentileJobDataFlow;
import com.latticeengines.scoring.workflow.steps.CalculatePredictedRevenuePercentileDataFlow;
import com.latticeengines.scoring.workflow.steps.CalculatePredictedRevenuePercentileJobDataFlow;
import com.latticeengines.scoring.workflow.steps.CombineInputTableWithScoreDataFlow;
import com.latticeengines.scoring.workflow.steps.PivotScoreAndEventDataFlow;
import com.latticeengines.scoring.workflow.steps.RecalculateExpectedRevenueDataFlow;
import com.latticeengines.scoring.workflow.steps.RecalculateExpectedRevenueJobDataFlow;
import com.latticeengines.scoring.workflow.steps.RecalculatePercentileScoreDataFlow;
import com.latticeengines.scoring.workflow.steps.RecalculatePercentileScoreJobDataFlow;
import com.latticeengines.scoring.workflow.steps.ScoreEventTable;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.serviceflows.workflow.transformation.AddStandardAttributes;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("generateAIRatingWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateAIRatingWorkflow extends AbstractWorkflow<GenerateAIRatingWorkflowConfiguration> {

    @Inject
    private ExtractScoringTarget extractScoringTarget;

    @Inject
    private CreateCdlEventTableStep createCdlEventTable;

    @Inject
    private AddStandardAttributes addStandardAttributes;

    @Inject
    private MatchDataCloudWorkflow matchDataCloud;

    @Inject
    private ScoreEventTable scoreEventTable;

    // TODO: pending migration
    @Inject
    private RecalculatePercentileScoreDataFlow recalculatePercentileScore;

    @Inject
    private RecalculatePercentileScoreJobDataFlow recalculatePercentileScoreJob;

    // TODO: pending migration
    @Inject
    private CalculatePredictedRevenuePercentileDataFlow calculatePredictedRevenuePercentileDataFlow;

    @Inject
    private CalculatePredictedRevenuePercentileJobDataFlow calculatePredictedRevenuePercentileJob;

    // TODO: pending migration
    @Inject
    private RecalculateExpectedRevenueDataFlow recalculateExpectedRevenueDataFlow;
    @Inject
    private RecalculateExpectedRevenueJobDataFlow recalculateExpectedRevenueJob;

    // TODO: pending migration
    @Inject
    private CalculateExpectedRevenuePercentileDataFlow calculateExpectedRevenuePercentileDataFlow;

    @Inject
    private CalculateExpectedRevenuePercentileJobDataFlow calculateExpectedRevenuePercentileJob;

    @Inject
    private ScoreAggregateFlow scoreAggregate;

    @Inject
    private CombineInputTableWithScoreDataFlow combineInputTableWithScore;

    @Inject
    private PivotScoreAndEventDataFlow pivotScoreAndEvent;

    @Override
    public Workflow defineWorkflow(GenerateAIRatingWorkflowConfiguration config) {
        WorkflowBuilder builder = new WorkflowBuilder(name(), config);
        boolean isLPI = CustomEventModelingType.LPI.equals(config.getCustomEventModelingType());
        if (!isLPI) {
            builder //
                    .next(extractScoringTarget) //
                    .next(createCdlEventTable); //
        }
        builder.next(matchDataCloud) //
                .next(addStandardAttributes) //
                .next(scoreEventTable); //
        if (!isLPI) {
            builder //
                    .next(recalculatePercentileScoreJob) //
                    .next(calculatePredictedRevenuePercentileJob) //
                    .next(recalculateExpectedRevenueJob) //
                    .next(calculateExpectedRevenuePercentileJob) //
                    .next(scoreAggregate); //
        }
        return builder //
                .next(combineInputTableWithScore) //
                .next(pivotScoreAndEvent) //
                .build();
    }

}
