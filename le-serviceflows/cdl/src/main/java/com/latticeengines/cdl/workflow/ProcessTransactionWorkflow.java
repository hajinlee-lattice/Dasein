package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.maintenance.SoftDeleteTransactionWrapper;
import com.latticeengines.cdl.workflow.steps.merge.MergeTransactionWrapper;
import com.latticeengines.cdl.workflow.steps.reset.ResetTransaction;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessTransactionWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processTransactionWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessTransactionWorkflow extends AbstractWorkflow<ProcessTransactionWorkflowConfiguration> {

    @Inject
    private SoftDeleteTransactionWrapper softDeleteTransactionWrapper;

    @Inject
    private MergeTransactionWrapper mergeTransactionWrapper;

    @Inject
    private UpdateTransactionWorkflow updateTransactionWorkflow;

    @Inject
    private RebuildTransactionWorkflow rebuildTransactionWorkflow;

    @Inject
    private ResetTransaction resetTransaction;

    @Override
    public Workflow defineWorkflow(ProcessTransactionWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(softDeleteTransactionWrapper) //
                .next(mergeTransactionWrapper) //
                .next(updateTransactionWorkflow) //
                .next(rebuildTransactionWorkflow) //
                .next(resetTransaction) //
                .build();
    }
}
