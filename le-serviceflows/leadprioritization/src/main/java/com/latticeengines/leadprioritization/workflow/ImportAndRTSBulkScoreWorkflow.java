package com.latticeengines.leadprioritization.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportAndRTSBulkScoreWorkflowConfiguration;
import com.latticeengines.scoring.workflow.RTSBulkScoreWorkflow;
import com.latticeengines.scoring.workflow.listeners.SendEmailAfterRTSBulkScoringCompletionListener;
import com.latticeengines.serviceflows.workflow.importdata.CreateTableImportReport;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("importAndRTSBulkScoreWorkflow")
@Lazy
public class ImportAndRTSBulkScoreWorkflow extends AbstractWorkflow<ImportAndRTSBulkScoreWorkflowConfiguration> {

    @Inject
    private ImportData importData;

    @Inject
    private CreateTableImportReport createTableImportReport;

    @Inject
    private RTSBulkScoreWorkflow rtsBulkScoreWorkflow;

    @Inject
    private SendEmailAfterRTSBulkScoringCompletionListener sendEmailAfterRTSBulkScoringCompletionListener;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(importData) //
                .next(createTableImportReport) //
                .next(rtsBulkScoreWorkflow)//
                .listener(sendEmailAfterRTSBulkScoringCompletionListener) //
                .build();
    }

}
