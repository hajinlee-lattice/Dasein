package com.latticeengines.cdl.workflow;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.ConsolidateTransactionData;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("consolidateTransactionWrapper")
public class ConsolidateTransactionWrapper extends BaseTransformationWrapper<ConsolidateTransactionData> {

    @Autowired
    private ConsolidateTransactionData consolidateTransactionData;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName("consolidateTransactionDataTransformStep");
    }

    @Override
    protected ConsolidateTransactionData getWrapperStep() {
        return consolidateTransactionData;
    }
}
