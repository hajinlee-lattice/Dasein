package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableStep;
import com.latticeengines.cdl.workflow.steps.ScoreAggregateFlow;
import com.latticeengines.cdl.workflow.steps.rating.CreateScoringTargetTable;
import com.latticeengines.cdl.workflow.steps.rating.ExtractScoringTarget;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.GenerateAIRatingWorkflowConfiguration;
import com.latticeengines.scoring.workflow.steps.CalculateExpectedRevenuePercentileDataFlow;
import com.latticeengines.scoring.workflow.steps.CalculatePredictedRevenuePercentileDataFlow;
import com.latticeengines.scoring.workflow.steps.CombineInputTableWithScoreDataFlow;
import com.latticeengines.scoring.workflow.steps.PivotScoreAndEventDataFlow;
import com.latticeengines.scoring.workflow.steps.RecalculateExpectedRevenueDataFlow;
import com.latticeengines.scoring.workflow.steps.RecalculatePercentileScoreDataFlow;
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
    private CreateScoringTargetTable createScoringTargetTable;

    @Inject
    private CreateCdlEventTableStep createCdlEventTable;

    @Inject
    private AddStandardAttributes addStandardAttributes;

    @Inject
    private MatchDataCloudWorkflow matchDataCloud;

    @Inject
    private ScoreEventTable scoreEventTable;

    @Inject
    private RecalculatePercentileScoreDataFlow recalculatePercentileScore;

    @Inject
    private CalculatePredictedRevenuePercentileDataFlow calculatePredictedRevenuePercentileDataFlow;

    @Inject
    private RecalculateExpectedRevenueDataFlow recalculateExpectedRevenueDataFlow;

    @Inject
    private CalculateExpectedRevenuePercentileDataFlow calculateExpectedRevenuePercentileDataFlow;

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
//                    .next(createScoringTargetTable) //
                    .next(createCdlEventTable); //
        }
        builder.next(matchDataCloud) //
                .next(addStandardAttributes) //
                .next(scoreEventTable); //
        if (!isLPI) {
            builder //
                    .next(recalculatePercentileScore) //
                    .next(calculatePredictedRevenuePercentileDataFlow) //
                    .next(recalculateExpectedRevenueDataFlow) //
                    .next(calculateExpectedRevenuePercentileDataFlow) //
                    .next(scoreAggregate); //
        }
        return builder //
                .next(combineInputTableWithScore) //
                .next(pivotScoreAndEvent) //
                .build();
    }

}
