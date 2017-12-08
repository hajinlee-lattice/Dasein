package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("aggregateTransactionWrapper")
public class AggregateTransactionWrapper extends BaseTransformationWrapper<AggregateTransaction> {

    @Inject
    private AggregateTransaction aggregateTransaction;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(AggregateTransaction.BEAN_NAME);
    }

    @Override
    protected AggregateTransaction getWrapperStep() {
        return aggregateTransaction;
    }

}
