package com.latticeengines.leadprioritization.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.leadprioritization.workflow.steps.AddStandardAttributes;
import com.latticeengines.leadprioritization.workflow.steps.CombineInputTableWithScoreDataFlow;
import com.latticeengines.leadprioritization.workflow.steps.ScoreEventTable;
import com.latticeengines.serviceflows.workflow.export.ExportWorkflow;
import com.latticeengines.serviceflows.workflow.match.MatchWorkflow;
import com.latticeengines.serviceflows.workflow.util.WriteOutput;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("scoreWorkflow")
public class ScoreWorkflow extends AbstractWorkflow<ScoreWorkflowConfiguration> {

    @Autowired
    private MatchWorkflow matchWorkflow;

    @Autowired
    private AddStandardAttributes addStandardAttributes;

    @Autowired
    private ScoreEventTable score;

    @Autowired
    private CombineInputTableWithScoreDataFlow combineInputTableWithScore;

    @Autowired
    private ExportWorkflow exportWorkflow;

    @Autowired
    private WriteOutput writeOutput;

    @Bean
    public Job scoreWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(matchWorkflow) //
                .next(addStandardAttributes) //
                .next(score) //
                .next(combineInputTableWithScore) //
                .next(exportWorkflow) //
                .next(writeOutput) //
                .build();

    }
}
