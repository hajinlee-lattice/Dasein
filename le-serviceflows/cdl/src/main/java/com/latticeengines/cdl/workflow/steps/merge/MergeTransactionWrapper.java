package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("mergeTransactionWrapper")
public class MergeTransactionWrapper extends BaseTransformationWrapper<MergeTransaction> {

    @Inject
    private MergeTransaction mergeTransaction;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(MergeTransaction.BEAN_NAME);
    }

    @Override
    protected MergeTransaction getWrapperStep() {
        return mergeTransaction;
    }

}
