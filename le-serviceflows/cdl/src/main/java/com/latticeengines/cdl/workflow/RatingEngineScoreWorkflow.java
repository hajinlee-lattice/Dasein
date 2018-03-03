package com.latticeengines.cdl.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableStep;
import com.latticeengines.cdl.workflow.steps.CreateCdlTargetTableFilterStep;
import com.latticeengines.cdl.workflow.steps.ScoreAggregateFlow;
import com.latticeengines.domain.exposed.serviceflows.cdl.RatingEngineScoreWorkflowConfiguration;
import com.latticeengines.scoring.workflow.listeners.SendEmailAfterScoringCompletionListener;
import com.latticeengines.scoring.workflow.steps.CombineInputTableWithScoreDataFlow;
import com.latticeengines.scoring.workflow.steps.ScoreEventTable;
import com.latticeengines.serviceflows.workflow.export.ExportWorkflow;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("ratingEngineScoreWorkflow")
@Lazy
public class RatingEngineScoreWorkflow extends AbstractWorkflow<RatingEngineScoreWorkflowConfiguration> {

    @Autowired
    private CreateCdlTargetTableFilterStep createCdlScoringTableFilterStep;

    @Autowired
    private CreateCdlEventTableStep createCdlEventTableStep;

    @Autowired
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Autowired
    private ScoreEventTable score;

    @Autowired
    private CombineInputTableWithScoreDataFlow combineInputTableWithScore;

    @Autowired
    private ScoreAggregateFlow scoreAggregateFlow;

    @Autowired
    private ExportWorkflow exportWorkflow;

    @Autowired
    private SendEmailAfterScoringCompletionListener sendEmailAfterScoringCompletionListener;

    @Override
    public Workflow defineWorkflow(RatingEngineScoreWorkflowConfiguration config) {
        return new WorkflowBuilder().next(createCdlScoringTableFilterStep) //
                .next(createCdlEventTableStep) //
                .next(matchDataCloudWorkflow) //
                .next(score) //
                .next(scoreAggregateFlow) //
                .next(combineInputTableWithScore) //
                .next(exportWorkflow) //
                .listener(sendEmailAfterScoringCompletionListener) //
                .build();

    }
}
