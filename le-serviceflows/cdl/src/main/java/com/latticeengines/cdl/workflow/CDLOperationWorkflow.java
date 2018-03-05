package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.MaintenanceOperationListener;
import com.latticeengines.cdl.workflow.steps.maintenance.DeleteFileUploadStep;
import com.latticeengines.cdl.workflow.steps.maintenance.OperationExecuteStep;
import com.latticeengines.cdl.workflow.steps.maintenance.StartMaintenanceStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLOperationWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cdlOperationWorkflow")
@Lazy
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

    @Override
    public Workflow defineWorkflow(CDLOperationWorkflowConfiguration config) {
        return new WorkflowBuilder()//
                .next(deleteFileUploadStep)//
                .next(startMaintenanceStep)//
                .next(cleanupByUploadWrapper, null)//
                .next(operationExecuteStep)//
                .listener(maintenanceOperationListener)//
                .build();
    }
}
