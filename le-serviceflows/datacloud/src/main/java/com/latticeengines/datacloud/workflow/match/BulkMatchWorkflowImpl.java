package com.latticeengines.datacloud.workflow.match;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.match.listeners.UpdateFailedMatchListener;
import com.latticeengines.datacloud.workflow.match.steps.ParallelBlockExecution;
import com.latticeengines.datacloud.workflow.match.steps.PrepareBulkMatchInput;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.match.BulkMatchWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("bulkMatchWorkflow")
public class BulkMatchWorkflowImpl extends AbstractWorkflow<BulkMatchWorkflowConfiguration>
        implements BulkMatchWorkflow {

    @Autowired
    private PrepareBulkMatchInput prepareBulkMatchInput;

    @Autowired
    private ParallelBlockExecution parallelBlockExecution;

    @Autowired
    private UpdateFailedMatchListener updateFailedMatchListener;

    @Override
    public Workflow defineWorkflow(BulkMatchWorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(prepareBulkMatchInput) //
                .next(parallelBlockExecution) //
                .listener(updateFailedMatchListener) //
                .build();
    }

}
