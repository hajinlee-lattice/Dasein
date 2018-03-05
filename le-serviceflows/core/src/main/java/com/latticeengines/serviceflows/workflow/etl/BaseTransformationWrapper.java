package com.latticeengines.serviceflows.workflow.etl;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.workflow.exposed.build.WorkflowWrapper;

public abstract class BaseTransformationWrapper<S extends BaseWrapperStepConfiguration, T extends BaseTransformWrapperStep<S>>
        extends WorkflowWrapper<S, TransformationWorkflowConfiguration, T, TransformationWorkflow> {

    @Autowired
    private TransformationWorkflow transformationWorkflow;

    @Override
    public TransformationWorkflow getWrappedWorkflow() {
        return transformationWorkflow;
    }

    protected void setTransformationStepBeanName(String transformationStepBeanName) {
        transformationWorkflow.getTransformationStep().setBeanName(transformationStepBeanName);
    }

}
