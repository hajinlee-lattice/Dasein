package com.latticeengines.cdl.workflow.steps.update;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("processTransactionDiffWrapper")
public class ProcessTransactionDiffWrapper extends BaseTransformationWrapper<ProcessTransactionDiff> {

    @Inject
    private ProcessTransactionDiff processTransactionDiff;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProcessTransactionDiff.BEAN_NAME);
    }

    @Override
    protected ProcessTransactionDiff getWrapperStep() {
        return processTransactionDiff;
    }

}
