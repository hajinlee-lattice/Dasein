package com.latticeengines.leadprioritization.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ScoreWorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.listeners.SendEmailAfterScoringCompletionListener;
import com.latticeengines.leadprioritization.workflow.steps.AddStandardAttributes;
import com.latticeengines.leadprioritization.workflow.steps.CombineInputTableWithScoreDataFlow;
import com.latticeengines.leadprioritization.workflow.steps.CombineMatchDebugWithScoreDataFlow;
import com.latticeengines.leadprioritization.workflow.steps.ScoreEventTable;
import com.latticeengines.serviceflows.workflow.export.ExportWorkflow;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("scoreWorkflow")
public class ScoreWorkflow extends AbstractWorkflow<ScoreWorkflowConfiguration> {

    @Autowired
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Autowired
    private AddStandardAttributes addStandardAttributesDataFlow;

    @Autowired
    private ScoreEventTable score;

    @Autowired
    private CombineMatchDebugWithScoreDataFlow combineMatchDebugWithScore;

    @Autowired
    private CombineInputTableWithScoreDataFlow combineInputTableWithScore;

    @Autowired
    private ExportWorkflow exportWorkflow;

    @Autowired
    private SendEmailAfterScoringCompletionListener sendEmailAfterScoringCompletionListener;

    @Bean
    public Job scoreWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(matchDataCloudWorkflow) //
                .next(addStandardAttributesDataFlow) //
                .next(score) //
                .next(combineMatchDebugWithScore) //
                .next(combineInputTableWithScore) //
                .next(exportWorkflow) //
                .listener(sendEmailAfterScoringCompletionListener) //
                .build();

    }
}
