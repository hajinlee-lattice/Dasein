package com.latticeengines.datacloud.workflow.engine;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.engine.steps.Publish;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.PublishWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("publishWorkflow")
public class PublishWorkflow extends AbstractWorkflow<PublishWorkflowConfiguration> {

    @Autowired
    private Publish publish;

    @Override
    public Workflow defineWorkflow(PublishWorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(publish) //
                .build();
    }

}
