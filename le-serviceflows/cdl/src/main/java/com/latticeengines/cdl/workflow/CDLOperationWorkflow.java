package com.latticeengines.cdl.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
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
public class CDLOperationWorkflow extends AbstractWorkflow<CDLOperationWorkflowConfiguration> {

    @Autowired
    private DeleteFileUploadStep deleteFileUploadStep;

    @Autowired
    private StartMaintenanceStep startMaintenanceStep;

    @Autowired
    private OperationExecuteStep operationExecuteStep;

    @Autowired
    private CleanupByUploadWrapper cleanupByUploadWrapper;

    @Autowired
    private MaintenanceOperationListener maintenanceOperationListener;

    @Bean
    public Job cdlDataFeedImportWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder()
                .next(deleteFileUploadStep)
                .next(startMaintenanceStep)
                .next(cleanupByUploadWrapper)
                .next(operationExecuteStep)
                .listener(maintenanceOperationListener)
                .build();
    }
}
