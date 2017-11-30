package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processContactWorkflow")
public class ProcessContactWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private ConsolidateContactWrapper consolidateContactWrapper;

    @Inject
    private SortContactWrapper sortContactWrapper;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(consolidateContactWrapper) //
                .next(sortContactWrapper)//
                .build();
    }
}
