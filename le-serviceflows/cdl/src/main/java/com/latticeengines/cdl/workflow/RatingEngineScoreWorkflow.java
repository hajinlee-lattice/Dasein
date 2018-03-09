package com.latticeengines.cdl.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableStep;
import com.latticeengines.cdl.workflow.steps.CreateCdlTargetTableFilterStep;
import com.latticeengines.cdl.workflow.steps.ScoreAggregateFlow;
import com.latticeengines.domain.exposed.serviceflows.cdl.RatingEngineScoreWorkflowConfiguration;
import com.latticeengines.scoring.workflow.listeners.SendEmailAfterScoringCompletionListener;
import com.latticeengines.scoring.workflow.steps.CombineInputTableWithScoreDataFlow;
import com.latticeengines.scoring.workflow.steps.ScoreEventTable;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("ratingEngineScoreWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
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
    private ExportData exportData;

    @Autowired
    private SendEmailAfterScoringCompletionListener sendEmailAfterScoringCompletionListener;

    @Override
    public Workflow defineWorkflow(RatingEngineScoreWorkflowConfiguration config) {
        return new WorkflowBuilder(name())//
                .next(createCdlScoringTableFilterStep) //
                .next(createCdlEventTableStep) //
                .next(matchDataCloudWorkflow, null) //
                .next(score) //
                .next(scoreAggregateFlow) //
                .next(combineInputTableWithScore) //
                .next(exportData) //
                .listener(sendEmailAfterScoringCompletionListener) //
                .build();

    }
}
