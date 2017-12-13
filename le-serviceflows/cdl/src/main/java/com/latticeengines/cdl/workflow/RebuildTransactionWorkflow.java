package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rebuild.ClonePeriodStores;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfilePurchaseHistoryWrapper;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfileTransactionWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("rebuildTransactionWorkflow")
public class RebuildTransactionWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private ClonePeriodStores clonePeriodStores;

    @Inject
    private ProfileTransactionWrapper profileTransactionWrapper;

    @Inject
    private ProfilePurchaseHistoryWrapper profilePurchaseHistoryWrapper;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(clonePeriodStores) //
                .next(profileTransactionWrapper) //
                .next(profilePurchaseHistoryWrapper) //
                .build();
    }
}
