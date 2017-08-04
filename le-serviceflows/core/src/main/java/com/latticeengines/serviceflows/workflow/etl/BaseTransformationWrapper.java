package com.latticeengines.serviceflows.workflow.etl;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.serviceflows.workflow.core.WorkflowWrapper;

public abstract class BaseTransformationWrapper<T extends BaseTransformWrapperStep>
        extends WorkflowWrapper<BaseTransformWrapperStep, TransformationWorkflow> {

    @Autowired
    private TransformationWorkflow transformationWorkflow;

    @Override
    protected TransformationWorkflow getWrappedWorkflow() {
        return transformationWorkflow;
    }

    protected void setTransformationStepBeanName(String transformationStepBeanName) {
        transformationWorkflow.getTransformationStep().setBeanName(transformationStepBeanName);
    }

}
