package com.latticeengines.cdl.workflow;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.ConsolidateProductData;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("consolidateProductWrapper")
public class ConsolidateProductWrapper extends BaseTransformationWrapper<ConsolidateProductData> {

    @Autowired
    private ConsolidateProductData consolidateProductData;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName("consolidateProductDataTransformStep");
    }

    @Override
    protected ConsolidateProductData getWrapperStep() {
        return consolidateProductData;
    }
}
