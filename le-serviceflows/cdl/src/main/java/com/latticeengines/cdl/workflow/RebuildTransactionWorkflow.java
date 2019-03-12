package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rebuild.ProfilePurchaseHistoryWrapper;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfileTransactionWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.RebuildTransactionWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("rebuildTransactionWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RebuildTransactionWorkflow extends AbstractWorkflow<RebuildTransactionWorkflowConfiguration> {

    @Inject
    private ProfileTransactionWrapper profileTransactionWrapper;

    @Inject
    private ProfilePurchaseHistoryWrapper profilePurchaseHistoryWrapper;

    @Override
    public Workflow defineWorkflow(RebuildTransactionWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(profileTransactionWrapper) //
                .next(profilePurchaseHistoryWrapper) //
                .build();
    }
}
