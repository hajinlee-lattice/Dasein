package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("profileTransactionWrapper")
public class ProfileTransactionWrapper extends BaseTransformationWrapper<ProfileTransaction> {

    @Inject
    private ProfileTransaction aggregateTransaction;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProfileTransaction.BEAN_NAME);
    }

    @Override
    protected ProfileTransaction getWrapperStep() {
        return aggregateTransaction;
    }

}
