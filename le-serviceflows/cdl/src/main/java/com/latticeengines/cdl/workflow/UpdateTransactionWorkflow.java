package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import com.latticeengines.cdl.workflow.steps.update.CloneTransaction;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.update.ClonePurchaseHistory;
import com.latticeengines.cdl.workflow.steps.update.ProcessTransactionDiff;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("updateTransactionWorkflow")
public class UpdateTransactionWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private CloneTransaction cloneTransaction;

    @Inject
    private ClonePurchaseHistory clonePurchaseHistory;

    @Inject
    private ProcessTransactionDiff processTransactionDiff;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(cloneTransaction)
                .next(clonePurchaseHistory) //
                .next(processTransactionDiff) //
                .build();
    }
}
