package com.latticeengines.cdl.workflow;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processAccountWorkflow")
public class ProcessAccountWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .build();
    }
}
