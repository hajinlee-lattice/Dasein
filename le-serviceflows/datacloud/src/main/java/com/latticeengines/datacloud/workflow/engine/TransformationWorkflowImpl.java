package com.latticeengines.datacloud.workflow.engine;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.engine.steps.TransformationStep;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.etl.TransformationWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("transformationWorkflow")
public class TransformationWorkflowImpl extends AbstractWorkflow<TransformationWorkflowConfiguration>
        implements TransformationWorkflow {

    @Autowired
    private TransformationStep transformationStep;

    @Bean
    public Job transformationWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(transformationStep) //
                .build();
    }
}
