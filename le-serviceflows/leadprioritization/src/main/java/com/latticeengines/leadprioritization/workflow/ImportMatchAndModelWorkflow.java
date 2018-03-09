package com.latticeengines.leadprioritization.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.modeling.workflow.ModelDataValidationWorkflow;
import com.latticeengines.modeling.workflow.ModelWorkflow;
import com.latticeengines.modeling.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.modeling.workflow.steps.DedupEventTable;
import com.latticeengines.scoring.workflow.RTSBulkScoreWorkflow;
import com.latticeengines.scoring.workflow.steps.PivotScoreAndEventDataFlow;
import com.latticeengines.scoring.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.importdata.CreateTableImportReport;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.serviceflows.workflow.transformation.AddStandardAttributes;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("importMatchAndModelWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportMatchAndModelWorkflow extends AbstractWorkflow<ImportMatchAndModelWorkflowConfiguration> {
    @Inject
    private ImportData importData;

    @Inject
    private CreateTableImportReport createTableImportReport;

    @Inject
    private DedupEventTable dedupEventTableDataFlow;

    @Inject
    private ModelDataValidationWorkflow modelValidationWorkflow;

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Inject
    private AddStandardAttributes addStandardAttributesDataFlow;

    @Inject
    private ModelWorkflow modelWorkflow;

    @Inject
    private SetConfigurationForScoring setConfigurationForScoring;

    @Inject
    private RTSBulkScoreWorkflow rtsBulkScoreWorkflow;

    @Inject
    private PivotScoreAndEventDataFlow pivotScoreAndEventDataFlow;

    @Inject
    private ExportData exportData;

    @Inject
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Override
    public Workflow defineWorkflow(ImportMatchAndModelWorkflowConfiguration config) {
        return new WorkflowBuilder(name()) //
                .next(importData) //
                .next(createTableImportReport) //
                .next(modelValidationWorkflow, null) //
                .next(matchDataCloudWorkflow, null) //
                .next(dedupEventTableDataFlow) //
                .next(addStandardAttributesDataFlow) //
                .next(modelWorkflow, null) //
                .next(setConfigurationForScoring) //
                .next(rtsBulkScoreWorkflow, null) //
                .next(pivotScoreAndEventDataFlow) //
                .next(exportData) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }
}
