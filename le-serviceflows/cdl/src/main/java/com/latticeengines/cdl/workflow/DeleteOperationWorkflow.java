package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.maintenance.SoftDeleteAccountWrapper;
import com.latticeengines.cdl.workflow.steps.maintenance.SoftDeleteContactWrapper;
import com.latticeengines.cdl.workflow.steps.maintenance.SoftDeleteTransactionWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.DeleteOperationWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("deleteOperationWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DeleteOperationWorkflow extends AbstractWorkflow<DeleteOperationWorkflowConfiguration> {

    @Inject
    private SoftDeleteAccountWrapper softDeleteAccountWrapper;

    @Inject
    private SoftDeleteContactWrapper softDeleteContactWrapper;

    @Inject
    private SoftDeleteTransactionWrapper softDeleteTransactionWrapper;

    @Override
    public Workflow defineWorkflow(DeleteOperationWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig) //
                .next(softDeleteAccountWrapper) //
                .next(softDeleteContactWrapper) //
                .next(softDeleteTransactionWrapper) //
                .build();
    }
}
