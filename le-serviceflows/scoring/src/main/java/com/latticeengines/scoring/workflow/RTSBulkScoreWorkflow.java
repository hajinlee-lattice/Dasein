package com.latticeengines.scoring.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.scoring.RTSBulkScoreWorkflowConfiguration;
import com.latticeengines.scoring.workflow.listeners.SendEmailAfterRTSBulkScoringCompletionListener;
import com.latticeengines.scoring.workflow.steps.CombineInputTableWithScoreDataFlow;
import com.latticeengines.scoring.workflow.steps.CombineMatchDebugWithScoreDataFlow;
import com.latticeengines.scoring.workflow.steps.RTSScoreEventTable;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("rtsBulkScoreWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
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
    private ExportData exportData;

    @Inject
    private SendEmailAfterRTSBulkScoringCompletionListener sendEmailAfterRTSBulkScoringCompletionListener;

    @Override
    public Workflow defineWorkflow(RTSBulkScoreWorkflowConfiguration config) {
        return new WorkflowBuilder(name())//
                .next(matchDataCloudWorkflow, null)//
                .next(score) //
                .next(combineMatchDebugWithScore) //
                .next(combineInputTableWithScore) //
                .next(exportData) //
                .listener(sendEmailAfterRTSBulkScoringCompletionListener) //
                .build();
    }
}
