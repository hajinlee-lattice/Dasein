package com.latticeengines.leadprioritization.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.leadprioritization.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.leadprioritization.workflow.steps.AddStandardAttributes;
import com.latticeengines.leadprioritization.workflow.steps.DedupEventTable;
import com.latticeengines.leadprioritization.workflow.steps.PivotScoreAndEvent;
import com.latticeengines.leadprioritization.workflow.steps.ResolveMetadataFromUserRefinedAttributes;
import com.latticeengines.leadprioritization.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("modelAndEmailWorkflow")
public class MatchAndModelAndEmailWorkflow extends AbstractWorkflow<MatchAndModelWorkflowConfiguration> {

    @Autowired
    private DedupEventTable dedupEventTable;

    @Autowired
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Autowired
    private AddStandardAttributes addStandardAttributes;

    @Autowired
    private ResolveMetadataFromUserRefinedAttributes resolveMetadataFromUserRefinedAttributes;

    @Autowired
    private ModelWorkflow modelWorkflow;

    @Autowired
    private SetConfigurationForScoring setConfigurationForScoring;

    @Autowired
    private ScoreWorkflow scoreWorkflow;

    @Autowired
    private PivotScoreAndEvent pivotScoreAndEvent;

    @Autowired
    private ExportData exportData;

    @Autowired
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Bean
    public Job modelAndEmailWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(matchDataCloudWorkflow) //
                .next(dedupEventTable) //
                .next(addStandardAttributes) //
                .next(resolveMetadataFromUserRefinedAttributes) //
                .next(modelWorkflow) //
                .next(setConfigurationForScoring) //
                .next(scoreWorkflow) //
                .next(pivotScoreAndEvent) //
                .next(exportData) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }
}
