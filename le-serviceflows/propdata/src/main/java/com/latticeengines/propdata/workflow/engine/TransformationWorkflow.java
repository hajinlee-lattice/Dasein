package com.latticeengines.propdata.workflow.engine;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.workflow.engine.steps.TransformationStepExecution;
import com.latticeengines.propdata.workflow.match.listeners.UpdateFailedMatchListener;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("propdataTransformationWorkflow")
public class TransformationWorkflow extends AbstractWorkflow<TransformationWorkflowConfiguration> {

    @Autowired
    private TransformationStepExecution transformationStepExecution;

    @Autowired
    private UpdateFailedMatchListener updateFailedMatchListener;

    @Bean
    public Job transformationWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(transformationStepExecution) //
                .listener(updateFailedMatchListener) //
                .build();
    }
}
