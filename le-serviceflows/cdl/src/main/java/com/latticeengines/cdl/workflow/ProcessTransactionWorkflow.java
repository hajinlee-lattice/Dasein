package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processTransactionWorkflow")
public class ProcessTransactionWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private ConsolidateTransactionWrapper consolidateTransactionWrapper;

    @Inject
    private CalculatePurchaseHistoryWrapper calculatePurchaseHistoryWrapper;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(consolidateTransactionWrapper) //
                .next(calculatePurchaseHistoryWrapper)//
                .build();
    }
}
