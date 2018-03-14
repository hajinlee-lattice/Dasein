package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.update.ClonePurchaseHistory;
import com.latticeengines.cdl.workflow.steps.update.CloneTransaction;
import com.latticeengines.cdl.workflow.steps.update.ProcessTransactionDiffWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.UpdateTransactionWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("updateTransactionWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateTransactionWorkflow extends AbstractWorkflow<UpdateTransactionWorkflowConfiguration> {

    @Inject
    private CloneTransaction cloneTransaction;

    @Inject
    private ClonePurchaseHistory clonePurchaseHistory;

    @Inject
    private ProcessTransactionDiffWrapper processTransactionDiffWrapper;

    @Override
    public Workflow defineWorkflow(UpdateTransactionWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(cloneTransaction) //
                .next(clonePurchaseHistory) //
                .next(processTransactionDiffWrapper) //
                .build();
    }
}
