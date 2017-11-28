package com.latticeengines.leadprioritization.workflow;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportAndRTSBulkScoreWorkflowConfiguration;
import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.leadprioritization.workflow.steps.CreateTableImportReport;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.serviceflows.workflow.listeners.SendEmailAfterRTSBulkScoringCompletionListener;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("importAndRTSBulkScoreWorkflow")
public class ImportAndRTSBulkScoreWorkflow extends AbstractWorkflow<ImportAndRTSBulkScoreWorkflowConfiguration> {

    @Autowired
    private ImportData importData;

    @Autowired
    private CreateTableImportReport createTableImportReport;

    @Autowired
    private RTSBulkScoreWorkflow rtsBulkScoreWorkflow;

    @Autowired
    private SendEmailAfterRTSBulkScoringCompletionListener sendEmailAfterRTSBulkScoringCompletionListener;

    @Bean
    public Job importAndRTSBulkScoreWorkflowJob() throws Exception {
        return buildWorkflow();
    }

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
