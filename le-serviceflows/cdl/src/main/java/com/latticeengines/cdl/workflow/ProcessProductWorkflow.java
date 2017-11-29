package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processProductWorkflow")
public class ProcessProductWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private ConsolidateProductWrapper consolidateProductWrapper;

    @Inject
    private SortProductWrapper sortProductWrapper;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(consolidateProductWrapper) //
                .next(sortProductWrapper)//
                .build();
    }
}
