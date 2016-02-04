package com.latticeengines.leadprioritization.workflow;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;

@Component("importAndScoreWorkflow")
public class ImportAndScoreWorkflow extends AbstractWorkflow<WorkflowConfiguration> {
    @Override
    public Workflow defineWorkflow() {
        return null;
    }
}
