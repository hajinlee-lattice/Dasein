package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.RegisterDeleteDataListener;
import com.latticeengines.cdl.workflow.steps.maintenance.DeleteFileUploadStep;
import com.latticeengines.cdl.workflow.steps.maintenance.RenameAndMatchStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.RegisterDeleteDataWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportDataFeedImportToS3;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("registerDeleteDataWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RegisterDeleteDataWorkflow extends AbstractWorkflow<RegisterDeleteDataWorkflowConfiguration> {

    @Inject
    private DeleteFileUploadStep deleteFileUploadStep;

    // When system IDs are used, go through this step to convert system IDs into
    // Lattice account/contact Id
    @Inject
    private RenameAndMatchStep renameAndMatchStep;

    @Inject
    private ExportDataFeedImportToS3 exportDataFeedImportToS3;

    @Inject
    private RegisterDeleteDataListener registerDeleteDataListener;

    @Override
    public Workflow defineWorkflow(RegisterDeleteDataWorkflowConfiguration workflowConfig) {

        return new WorkflowBuilder(name(), workflowConfig) //
                .next(deleteFileUploadStep) //
                .next(renameAndMatchStep) //
                .next(exportDataFeedImportToS3) //
                .listener(registerDeleteDataListener) //
                .build();
    }
}
