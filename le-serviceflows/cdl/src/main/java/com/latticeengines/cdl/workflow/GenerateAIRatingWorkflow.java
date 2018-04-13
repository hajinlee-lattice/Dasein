package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableStep;
import com.latticeengines.cdl.workflow.steps.ScoreAggregateFlow;
import com.latticeengines.cdl.workflow.steps.rating.CreateCrossSellScoringTargetTable;
import com.latticeengines.cdl.workflow.steps.rating.CreateCustomEventScoringTargetTable;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.GenerateAIRatingWorkflowConfiguration;
import com.latticeengines.scoring.workflow.steps.CombineFilterTableDataFlow;
import com.latticeengines.scoring.workflow.steps.CombineInputTableWithScoreDataFlow;
import com.latticeengines.scoring.workflow.steps.ComputeLiftDataFlow;
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
    private CreateCrossSellScoringTargetTable createCrossSellScoringTargetTable;

    @Inject
    private CreateCustomEventScoringTargetTable createCustomEventScoringTargetTable;

    @Inject
    private CombineFilterTableDataFlow combineFilterTable;

    @Inject
    private CreateCdlEventTableStep createCdlEventTable;

    @Inject
    private AddStandardAttributes addStandardAttributes;

    @Inject
    private MatchDataCloudWorkflow matchDataCloud;

    @Inject
    private ScoreEventTable scoreEventTable;

    @Inject
    private ScoreAggregateFlow scoreAggregate;

    @Inject
    private CombineInputTableWithScoreDataFlow combineInputTableWithScore;

    @Inject
    private ComputeLiftDataFlow computeLift;

    @Override
    public Workflow defineWorkflow(GenerateAIRatingWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(createCrossSellScoringTargetTable) //
                .next(createCustomEventScoringTargetTable) //
                .next(combineFilterTable) //
                .next(createCdlEventTable) //
                .next(matchDataCloud) //
                .next(addStandardAttributes) //
                .next(scoreEventTable) //
                .next(scoreAggregate) //
                .next(combineInputTableWithScore) //
                .next(computeLift) //
                .build();
    }

}
