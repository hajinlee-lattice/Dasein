package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.maintenance.SoftDeleteAccountWrapper;
import com.latticeengines.cdl.workflow.steps.merge.GenerateAccountLookup;
import com.latticeengines.cdl.workflow.steps.merge.MergeAccountWrapper;
import com.latticeengines.cdl.workflow.steps.reset.ResetAccount;
import com.latticeengines.cdl.workflow.steps.validations.ValidateAccountBatchStore;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAccountWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processAccountWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessAccountWorkflow extends AbstractWorkflow<ProcessAccountWorkflowConfiguration> {

    @Inject
    private SoftDeleteAccountWrapper softDeleteAccountWrapper;

    @Inject
    private MergeAccountWrapper mergeAccountWrapper;

    @Inject
    private ValidateAccountBatchStore validateAccountBatchStore;

    @Inject
    private GenerateAccountLookup generateAccountLookup;

    @Inject
    private UpdateAccountWorkflow updateAccountWorkflow;

    @Inject
    private RebuildAccountWorkflow rebuildAccountWorkflow;

    @Inject
    private ResetAccount resetAccount;

    @Override
    public Workflow defineWorkflow(ProcessAccountWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(softDeleteAccountWrapper) //
                .next(mergeAccountWrapper) //
                .next(validateAccountBatchStore) //
                .next(generateAccountLookup) //
                .next(updateAccountWorkflow) //
                .next(rebuildAccountWorkflow) //
                .next(resetAccount) //
                .build();
    }
}
