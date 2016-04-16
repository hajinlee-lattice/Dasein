package com.latticeengines.propdata.workflow.engine;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.workflow.engine.steps.PrepareTransformationStepInput;
import com.latticeengines.propdata.workflow.engine.steps.TransformationProceedProgressStepExecution;
import com.latticeengines.propdata.workflow.match.listeners.UpdateFailedMatchListener;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("propdataTransformationProceedProgressWorkflow")
public class TransformationProceedProgressWorkflow extends AbstractWorkflow<TransformationWorkflowConfiguration> {

    @Autowired
    private PrepareTransformationStepInput prepareTransformationStepInput;

    @Autowired
    private TransformationProceedProgressStepExecution transformationProceedProgressStepExecution;

    @Autowired
    private UpdateFailedMatchListener updateFailedMatchListener;

    @Bean
    public Job transformationProceedProgressWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(prepareTransformationStepInput) //
                .next(transformationProceedProgressStepExecution) //
                .listener(updateFailedMatchListener) //
                .build();
    }
}
