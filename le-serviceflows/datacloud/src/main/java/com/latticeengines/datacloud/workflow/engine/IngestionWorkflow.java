package com.latticeengines.datacloud.workflow.engine;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.engine.steps.IngestionStep;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.IngestionWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("ingestionWorkflow")
public class IngestionWorkflow extends AbstractWorkflow<IngestionWorkflowConfiguration> {
    @Autowired
    private IngestionStep ingestionStep;

    @Override
    public Workflow defineWorkflow(IngestionWorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(ingestionStep) //
                .build();
    }

}
