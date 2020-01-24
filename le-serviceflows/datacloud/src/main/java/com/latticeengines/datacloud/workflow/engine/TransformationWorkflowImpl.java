package com.latticeengines.datacloud.workflow.engine;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.engine.steps.TransformationStep;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.TransformationWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("transformationWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class TransformationWorkflowImpl extends AbstractWorkflow<TransformationWorkflowConfiguration>
        implements TransformationWorkflow {

    @Inject
    private TransformationStep transformationStep;

    @Override
    public Workflow defineWorkflow(TransformationWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(transformationStep) //
                .build();
    }

    @Override
    public BaseWorkflowStep<? extends BaseStepConfiguration> getTransformationStep() {
        return transformationStep;
    }
}
