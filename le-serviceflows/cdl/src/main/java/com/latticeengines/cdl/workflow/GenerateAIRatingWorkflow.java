package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableStep;
import com.latticeengines.cdl.workflow.steps.ScoreAggregateFlow;
import com.latticeengines.cdl.workflow.steps.rating.CreateScoringTargetTable;
import com.latticeengines.domain.exposed.serviceflows.cdl.GenerateRatingWorkflowConfiguration;
import com.latticeengines.scoring.workflow.steps.CombineInputTableWithScoreDataFlow;
import com.latticeengines.scoring.workflow.steps.ComputeLiftDataFlow;
import com.latticeengines.scoring.workflow.steps.ScoreEventTable;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("generateAIRatingsWorkflow")
@Lazy
public class GenerateAIRatingWorkflow extends AbstractWorkflow<GenerateRatingWorkflowConfiguration> {

    @Inject
    private CreateScoringTargetTable createScoringTargetTable;

    @Inject
    private CreateCdlEventTableStep createCdlEventTable;

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
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(createScoringTargetTable) //
                .next(createCdlEventTable) //
                .next(matchDataCloud) //
                .next(scoreEventTable) //
                .next(scoreAggregate) //
                .next(combineInputTableWithScore) //
                .next(computeLift) //
                .build();
    }

}
