package com.latticeengines.cdl.workflow.steps.update;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("processContactDiffWrapper")
public class ProcessContactDiffWrapper extends BaseTransformationWrapper<ProcessContactDiff> {

    @Inject
    private ProcessContactDiff processContactDiff;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProcessContactDiff.BEAN_NAME);
    }

    @Override
    protected ProcessContactDiff getWrapperStep() {
        return processContactDiff;
    }

}
