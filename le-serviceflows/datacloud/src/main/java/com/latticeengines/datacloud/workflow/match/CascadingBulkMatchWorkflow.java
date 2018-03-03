package com.latticeengines.datacloud.workflow.match;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.match.steps.CascadingBulkMatchStep;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.CascadingBulkMatchWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cascadingBulkMatchWorkflow")
public class CascadingBulkMatchWorkflow extends AbstractWorkflow<CascadingBulkMatchWorkflowConfiguration> {

    @Autowired
    private CascadingBulkMatchStep cascadingBulkMatchStep;

    @Override
    public Workflow defineWorkflow(CascadingBulkMatchWorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(cascadingBulkMatchStep) //
                .build();
    }
}
