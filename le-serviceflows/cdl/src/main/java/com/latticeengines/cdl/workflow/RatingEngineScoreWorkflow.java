package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

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
import com.latticeengines.serviceflows.workflow.transformation.AddStandardAttributes;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("ratingEngineScoreWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RatingEngineScoreWorkflow extends AbstractWorkflow<RatingEngineScoreWorkflowConfiguration> {

    @Inject
    private CreateCdlTargetTableFilterStep createCdlTargetTableFilterStep;

    @Inject
    private CreateCdlEventTableStep createCdlEventTableStep;

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Inject
    private AddStandardAttributes addStandardAttributesDataFlow;

    @Inject
    private ScoreEventTable score;

    @Inject
    private CombineInputTableWithScoreDataFlow combineInputTableWithScore;

    @Inject
    private ScoreAggregateFlow scoreAggregateFlow;

    @Inject
    private ExportData exportData;

    @Inject
    private SendEmailAfterScoringCompletionListener sendEmailAfterScoringCompletionListener;

    @Override
    public Workflow defineWorkflow(RatingEngineScoreWorkflowConfiguration config) {
        WorkflowBuilder builder = new WorkflowBuilder(name(), config);
        if (!config.isSkipImport()) {
            builder.next(createCdlTargetTableFilterStep) //
                    .next(createCdlEventTableStep);
        }
        builder.next(matchDataCloudWorkflow) //
                .next(addStandardAttributesDataFlow) //
                .next(score) //
                .next(scoreAggregateFlow) //
                .next(combineInputTableWithScore) //
                .next(exportData) //
                .listener(sendEmailAfterScoringCompletionListener);
        return builder.build();
    }
}
