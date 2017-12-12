package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.merge.MergeTransactionWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processTransactionWorkflow")
public class ProcessTransactionWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private MergeTransactionWrapper mergeTransactionWrapper;

    @Inject
    private UpdateTransactionWorkflow updateTransactionWorkflow;

    @Inject
    private RebuildTransactionWorkflow rebuildTransactionWorkflow;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(mergeTransactionWrapper) //
                .next(updateTransactionWorkflow) //
                .next(rebuildTransactionWorkflow) //
                .build();
    }
}
