package com.latticeengines.leadprioritization.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportMatchAndScoreWorkflowConfiguration;
import com.latticeengines.scoring.workflow.ScoreWorkflow;
import com.latticeengines.scoring.workflow.listeners.SendEmailAfterScoringCompletionListener;
import com.latticeengines.serviceflows.workflow.importdata.CreateTableImportReport;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("importMatchAndScoreWorkflow")
@Lazy
public class ImportMatchAndScoreWorkflow extends AbstractWorkflow<ImportMatchAndScoreWorkflowConfiguration> {

    @Inject
    private ImportData importData;

    @Inject
    private CreateTableImportReport createTableImportReport;

    @Inject
    private ScoreWorkflow scoreWorkflow;

    @Inject
    private SendEmailAfterScoringCompletionListener sendEmailAfterScoringCompletionListener;

    @Override
    public Workflow defineWorkflow(ImportMatchAndScoreWorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(importData) //
                .next(createTableImportReport) //
                .next(scoreWorkflow)//
                .listener(sendEmailAfterScoringCompletionListener) //
                .build();
    }

}
