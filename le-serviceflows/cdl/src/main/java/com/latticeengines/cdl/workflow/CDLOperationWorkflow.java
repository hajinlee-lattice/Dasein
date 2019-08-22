package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.MaintenanceOperationListener;
import com.latticeengines.cdl.workflow.steps.maintenance.DeleteFileUploadStep;
import com.latticeengines.cdl.workflow.steps.maintenance.OperationExecuteStep;
import com.latticeengines.cdl.workflow.steps.maintenance.StartMaintenanceStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLOperationWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportCleanupToS3;
import com.latticeengines.serviceflows.workflow.export.ImportCleanupFromS3;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cdlOperationWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CDLOperationWorkflow extends AbstractWorkflow<CDLOperationWorkflowConfiguration> {

    @Inject
    private DeleteFileUploadStep deleteFileUploadStep;

    @Inject
    private StartMaintenanceStep startMaintenanceStep;

    @Inject
    private OperationExecuteStep operationExecuteStep;

    @Inject
    private CleanupByUploadWrapper cleanupByUploadWrapper;

    @Inject
    private MaintenanceOperationListener maintenanceOperationListener;

    @Inject
    private ImportCleanupFromS3 importCleanupFromS3;

    @Inject
    private ExportCleanupToS3 exportCleanupToS3;

    @Override
    public Workflow defineWorkflow(CDLOperationWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config)//
                .next(startMaintenanceStep)//
                .next(importCleanupFromS3) //
                .next(deleteFileUploadStep)//
                .next(cleanupByUploadWrapper)//
                .next(operationExecuteStep)//
                .next(exportCleanupToS3) //
                .listener(maintenanceOperationListener)//
                .build();
    }
}
