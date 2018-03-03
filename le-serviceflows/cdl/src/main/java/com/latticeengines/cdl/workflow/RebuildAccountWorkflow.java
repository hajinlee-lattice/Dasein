package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rebuild.ProfileAccountWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("rebuildAccountWorkflow")
@Lazy
public class RebuildAccountWorkflow extends AbstractWorkflow<ProcessAnalyzeWorkflowConfiguration> {

    @Inject
    private ProfileAccountWrapper profileAccountWrapper;

    @Override
    public Workflow defineWorkflow(ProcessAnalyzeWorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(profileAccountWrapper) //
                .build();
    }
}
