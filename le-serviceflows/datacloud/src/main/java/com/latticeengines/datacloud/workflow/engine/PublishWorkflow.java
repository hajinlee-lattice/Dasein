package com.latticeengines.datacloud.workflow.engine;

import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.PublishWorkflowConfiguration;
import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.engine.steps.Publish;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("publishWorkflow")
public class PublishWorkflow extends AbstractWorkflow<PublishWorkflowConfiguration>  {

    @Autowired
    private Publish publish;

    @Bean
    public Job publishWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(publish) //
                .build();
    }

}
