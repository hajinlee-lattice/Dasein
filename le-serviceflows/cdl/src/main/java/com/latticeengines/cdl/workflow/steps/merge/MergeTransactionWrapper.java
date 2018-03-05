package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("mergeTransactionWrapper")
public class MergeTransactionWrapper
        extends BaseTransformationWrapper<ProcessTransactionStepConfiguration, MergeTransaction> {

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
