package com.latticeengines.scoring.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.scoring.RTSBulkScoreWorkflowConfiguration;
import com.latticeengines.scoring.workflow.listeners.SendEmailAfterRTSBulkScoringCompletionListener;
import com.latticeengines.scoring.workflow.steps.CombineInputTableWithScoreDataFlow;
import com.latticeengines.scoring.workflow.steps.CombineMatchDebugWithScoreDataFlow;
import com.latticeengines.scoring.workflow.steps.RTSScoreEventTable;
import com.latticeengines.serviceflows.workflow.export.ExportWorkflow;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("rtsBulkScoreWorkflow")
@Lazy
public class RTSBulkScoreWorkflow extends AbstractWorkflow<RTSBulkScoreWorkflowConfiguration> {

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Inject
    private RTSScoreEventTable score;

    @Inject
    private CombineInputTableWithScoreDataFlow combineInputTableWithScore;

    @Inject
    private CombineMatchDebugWithScoreDataFlow combineMatchDebugWithScore;

    @Inject
    private ExportWorkflow exportWorkflow;

    @Inject
    private SendEmailAfterRTSBulkScoringCompletionListener sendEmailAfterRTSBulkScoringCompletionListener;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(matchDataCloudWorkflow)//
                .next(score) //
                .next(combineMatchDebugWithScore) //
                .next(combineInputTableWithScore) //
                .next(exportWorkflow) //
                .listener(sendEmailAfterRTSBulkScoringCompletionListener) //
                .build();
    }
}
