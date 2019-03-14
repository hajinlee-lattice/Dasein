package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.LdcOnlyAttributesStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.CustomEventModelingWorkflowConfiguration;
import com.latticeengines.modeling.workflow.ModelDataValidationWorkflow;
import com.latticeengines.modeling.workflow.ModelWorkflow;
import com.latticeengines.modeling.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.modeling.workflow.steps.AttributeCategoryModifier;
import com.latticeengines.modeling.workflow.steps.DedupEventTable;
import com.latticeengines.modeling.workflow.steps.MergeUserRefinedAttributes;
import com.latticeengines.modeling.workflow.steps.UseConfiguredModelingAttributes;
import com.latticeengines.scoring.workflow.steps.ExportBucketTool;
import com.latticeengines.scoring.workflow.steps.ExportScoreTrainingFile;
import com.latticeengines.scoring.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.serviceflows.workflow.export.ExportModelToS3;
import com.latticeengines.serviceflows.workflow.export.ExportSourceFileToS3;
import com.latticeengines.serviceflows.workflow.export.ImportModelFromS3;
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
    private ExportSourceFileToS3 exportSourceFileToS3;

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
    private UseConfiguredModelingAttributes useConfiguredModelingAttributes;

    @Inject
    private LdcOnlyAttributesStep ldcOnlyAttributesDataFlow;

    @Inject
    private AttributeCategoryModifier attributeCategoryModifier;

    @Inject
    private MergeUserRefinedAttributes mergeUserRefinedAttributes;

    @Inject
    private ModelWorkflow modelWorkflow;

    @Inject
    private SetConfigurationForScoring setConfigurationForScoring;

    @Inject
    private GenerateAIRatingWorkflow generateRating;

    @Inject
    private ExportScoreTrainingFile exportScoreTrainingFile;

    @Inject
    private ExportBucketTool exportBucketTool;

    @Inject
    private ImportModelFromS3 importModelFromS3;

    @Inject
    private ExportModelToS3 modelExportToS3;

    @Inject
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Override
    public Workflow defineWorkflow(CustomEventModelingWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(importModelFromS3) //
                .next(importData) //
                .next(exportSourceFileToS3) //
                .next(createTableImportReport) //
                .next(modelValidationWorkflow) //
                .next(customEventMatchWorkflow) //
                .next(dedupEventTableDataFlow) //
                .next(addStandardAttributesDataFlow) //
                .next(useConfiguredModelingAttributes) //
                .next(ldcOnlyAttributesDataFlow) //
                .next(attributeCategoryModifier) //
                .next(mergeUserRefinedAttributes) //
                .next(modelWorkflow) //
                .next(setConfigurationForScoring) //
                .next(generateRating) //
                .next(exportScoreTrainingFile) //
                .next(exportBucketTool) //
                .next(modelExportToS3) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }

}
