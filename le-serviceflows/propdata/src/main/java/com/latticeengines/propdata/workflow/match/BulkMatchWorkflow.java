package com.latticeengines.propdata.workflow.match;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.workflow.match.listeners.UpdateFailedMatchListener;
import com.latticeengines.propdata.workflow.match.steps.ParallelBlockExecution;
import com.latticeengines.propdata.workflow.match.steps.PrepareBulkMatchInput;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("bulkMatchWorkflow")
public class BulkMatchWorkflow extends AbstractWorkflow<BulkMatchWorkflowConfiguration> {

    @Autowired
    private PrepareBulkMatchInput prepareBulkMatchInput;

    @Autowired
    private ParallelBlockExecution parallelBlockExecution;

    @Autowired
    private UpdateFailedMatchListener updateFailedMatchListener;

    @Bean
    public Job bulkMatchWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(prepareBulkMatchInput) //
                .next(parallelBlockExecution) //
                .listener(updateFailedMatchListener) //
                .build();
    }
}
