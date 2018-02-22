package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.merge.MergeAccountWrapper;
import com.latticeengines.cdl.workflow.steps.reset.ResetAccount;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processAccountWorkflow")
@Lazy
public class ProcessAccountWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private MergeAccountWrapper mergeAccountWrapper;

    @Inject
    private UpdateAccountWorkflow updateAccountWorkflow;

    @Inject
    private RebuildAccountWorkflow rebuildAccountWorkflow;

    @Inject
    private ResetAccount resetAccount;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(mergeAccountWrapper) //
                .next(updateAccountWorkflow) //
                .next(rebuildAccountWorkflow) //
                .next(resetAccount) //
                .build();
    }
}
