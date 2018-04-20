package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.ActivateRatingEngineListener;
import com.latticeengines.cdl.workflow.steps.LdcOnlyAttributesStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.CustomEventModelingWorkflowConfiguration;
import com.latticeengines.modeling.workflow.ModelDataValidationWorkflow;
import com.latticeengines.modeling.workflow.ModelWorkflow;
import com.latticeengines.modeling.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.modeling.workflow.steps.DedupEventTable;
import com.latticeengines.scoring.workflow.steps.PivotScoreAndEventDataFlow;
import com.latticeengines.scoring.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.importdata.CreateTableImportReport;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.serviceflows.workflow.transformation.AddStandardAttributes;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("customEventModelingWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CustomEventModelingWorkflow extends AbstractWorkflow<CustomEventModelingWorkflowConfiguration> {

    @Inject
    private ImportData importData;

    @Inject
    private CreateTableImportReport createTableImportReport;

    @Inject
    private ModelDataValidationWorkflow modelValidationWorkflow;

    @Inject
    private CustomEventMatchWorkflow customEventMatchWorkflow;

    @Inject
    private DedupEventTable dedupEventTableDataFlow;

    @Inject
    private AddStandardAttributes addStandardAttributesDataFlow;

    @Inject
    private LdcOnlyAttributesStep ldcOnlyAttributesDataFlow;

    @Inject
    private ModelWorkflow modelWorkflow;

    @Inject
    private SetConfigurationForScoring setConfigurationForScoring;

    @Inject
    private GenerateAIRatingWorkflow generateRating;

	@Inject
    private PivotScoreAndEventDataFlow pivotScoreAndEventDataFlow;
    
    @Inject
    private ExportData exportData;

    @Inject
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Inject
    private ActivateRatingEngineListener activateRatingEngineListener;

    @Override
    public Workflow defineWorkflow(CustomEventModelingWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(importData) //
                .next(createTableImportReport) //
                .next(modelValidationWorkflow) //
                .next(customEventMatchWorkflow) //
                .next(dedupEventTableDataFlow) //
                .next(addStandardAttributesDataFlow) //
                .next(ldcOnlyAttributesDataFlow) //
                .next(modelWorkflow) //
                .next(setConfigurationForScoring) //
                .next(generateRating) //
                .next(exportData) //
                .next(pivotScoreAndEventDataFlow) //
                .next(exportData) //
                .listener(activateRatingEngineListener) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }

}
