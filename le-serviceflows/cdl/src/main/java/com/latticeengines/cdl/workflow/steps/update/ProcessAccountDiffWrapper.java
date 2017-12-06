package com.latticeengines.cdl.workflow.steps.update;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("processAccountDiffWrapper")
public class ProcessAccountDiffWrapper extends BaseTransformationWrapper<ProcessAccountDiff> {

    @Inject
    private ProcessAccountDiff processAccountDiff;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProcessAccountDiff.BEAN_NAME);
    }

    @Override
    protected ProcessAccountDiff getWrapperStep() {
        return processAccountDiff;
    }

}
