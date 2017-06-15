package com.latticeengines.datacloud.workflow.engine;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.engine.steps.OrchestrationStep;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.OrchestrationWorkflowConfig;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("orchestrationWorkflow")
public class OrchestrationWorkflow extends AbstractWorkflow<OrchestrationWorkflowConfig> {
    @Autowired
    private OrchestrationStep orchestrationStep;

    @Bean
    public Job orchestrationWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(orchestrationStep) //
                .build();
    }
}
