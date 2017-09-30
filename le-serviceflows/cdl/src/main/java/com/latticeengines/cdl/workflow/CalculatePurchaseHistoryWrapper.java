package com.latticeengines.cdl.workflow;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CalculatePurchaseHistory;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("calculatePurchaseHistoryWrapper")
public class CalculatePurchaseHistoryWrapper extends BaseTransformationWrapper<CalculatePurchaseHistory> {

    @Autowired
    private CalculatePurchaseHistory calculatePurchaseHistory;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName("calculatePurchaseHistoryTransformStep");
    }

    @Override
    protected CalculatePurchaseHistory getWrapperStep() {
        return calculatePurchaseHistory;
    }
}
