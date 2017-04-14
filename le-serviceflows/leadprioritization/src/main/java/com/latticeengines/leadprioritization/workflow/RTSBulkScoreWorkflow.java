package com.latticeengines.leadprioritization.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.leadprioritization.workflow.listeners.SendEmailAfterRTSBulkScoringCompletionListener;
import com.latticeengines.leadprioritization.workflow.steps.CombineInputTableWithScoreDataFlow;
import com.latticeengines.leadprioritization.workflow.steps.CombineMatchDebugWithScoreDataFlow;
import com.latticeengines.leadprioritization.workflow.steps.RTSScoreEventTable;
import com.latticeengines.serviceflows.workflow.export.ExportWorkflow;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("rtsBulkScoreWorkflow")
public class RTSBulkScoreWorkflow extends AbstractWorkflow<RTSBulkScoreWorkflowConfiguration> {

    @Autowired
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Autowired
    private RTSScoreEventTable score;

    @Autowired
    private CombineInputTableWithScoreDataFlow combineInputTableWithScore;

    @Autowired
    private CombineMatchDebugWithScoreDataFlow combineMatchDebugWithScore;

    @Autowired
    private ExportWorkflow exportWorkflow;

    @Autowired
    private SendEmailAfterRTSBulkScoringCompletionListener sendEmailAfterRTSBulkScoringCompletionListener;

    @Bean
    public Job rtsBulkScoreWorkflowJob() throws Exception {
        return buildWorkflow();
    }

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
