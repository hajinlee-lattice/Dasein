package com.latticeengines.leadprioritization.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportAndRTSBulkScoreWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.RTSBulkScoreWorkflowConfiguration;
import com.latticeengines.scoring.workflow.RTSBulkScoreWorkflow;
import com.latticeengines.scoring.workflow.listeners.SendEmailAfterRTSBulkScoringCompletionListener;
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
    private RTSBulkScoreWorkflow rtsBulkScoreWorkflow;

    @Inject
    private SendEmailAfterRTSBulkScoringCompletionListener sendEmailAfterRTSBulkScoringCompletionListener;

    @Override
    public Workflow defineWorkflow(ImportAndRTSBulkScoreWorkflowConfiguration config) {
        return new WorkflowBuilder(name()) //
                .next(importData) //
                .next(createTableImportReport) //
                .next(rtsBulkScoreWorkflow, //
                        (RTSBulkScoreWorkflowConfiguration) config.getSubWorkflowConfigRegistry()
                                .get(RTSBulkScoreWorkflowConfiguration.class.getSimpleName()))//
                .listener(sendEmailAfterRTSBulkScoringCompletionListener) //
                .build();
    }

}
