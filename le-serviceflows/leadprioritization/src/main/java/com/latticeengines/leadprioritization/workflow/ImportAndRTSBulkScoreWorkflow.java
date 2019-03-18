package com.latticeengines.leadprioritization.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportAndRTSBulkScoreWorkflowConfiguration;
import com.latticeengines.scoring.workflow.RTSBulkScoreWorkflow;
import com.latticeengines.scoring.workflow.listeners.SendEmailAfterRTSBulkScoringCompletionListener;
import com.latticeengines.serviceflows.workflow.export.ExportScoreToS3;
import com.latticeengines.serviceflows.workflow.export.ExportSourceFileToS3;
import com.latticeengines.serviceflows.workflow.importdata.CreateTableImportReport;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("importAndRTSBulkScoreWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportAndRTSBulkScoreWorkflow extends AbstractWorkflow<ImportAndRTSBulkScoreWorkflowConfiguration> {

    @Inject
    private ImportData importData;

    @Inject
    private CreateTableImportReport createTableImportReport;

    @Inject
    private ExportSourceFileToS3 exportSourceFileToS3;

    @Inject
    private RTSBulkScoreWorkflow rtsBulkScoreWorkflow;

    @Inject
    private ExportScoreToS3 exportScoreToS3;

    @Inject
    private SendEmailAfterRTSBulkScoringCompletionListener sendEmailAfterRTSBulkScoringCompletionListener;

    @Override
    public Workflow defineWorkflow(ImportAndRTSBulkScoreWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(importData) //
                .next(exportSourceFileToS3) //
                .next(createTableImportReport) //
                .next(rtsBulkScoreWorkflow) //
                .next(exportScoreToS3) //
                .listener(sendEmailAfterRTSBulkScoringCompletionListener) //
                .build();
    }

}
