package com.latticeengines.leadprioritization.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.leadprioritization.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.leadprioritization.workflow.steps.AddStandardAttributes;
import com.latticeengines.leadprioritization.workflow.steps.CreateTableImportReport;
import com.latticeengines.leadprioritization.workflow.steps.DedupEventTable;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("importMatchAndModelWorkflow")
public class ImportMatchAndModelWorkflow extends AbstractWorkflow<ImportMatchAndModelWorkflowConfiguration> {
    @Autowired
    private ImportData importData;

    @Autowired
    private CreateTableImportReport createTableImportReport;

    @Autowired
    private DedupEventTable dedupEventTable;

    @Autowired
    private ModelDataValidationWorkflow modelValidationWorkflow;

    @Autowired
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Autowired
    private AddStandardAttributes addStandardAttributes;

    @Autowired
    private ModelWorkflow modelWorkflow;

    @Autowired
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Bean
    public Job importMatchAndModelWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(importData) //
                .next(createTableImportReport) //
                .next(dedupEventTable) //
                .next(modelValidationWorkflow) //
                .next(matchDataCloudWorkflow) //
                .next(addStandardAttributes) //
                .next(modelWorkflow) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }
}
