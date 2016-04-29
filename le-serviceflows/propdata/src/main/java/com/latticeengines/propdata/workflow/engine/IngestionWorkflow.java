package com.latticeengines.propdata.workflow.engine;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.workflow.engine.steps.IngestionStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("ingestionWorkflow")
public class IngestionWorkflow extends AbstractWorkflow<IngestionWorkflowConfiguration> {
    @Autowired
    private IngestionStep ingestionStep;

    @Bean
    public Job ingestionWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(ingestionStep) //
                .build();
    }

}
