package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("mergeProductWrapper")
public class MergeProductWrapper extends BaseTransformationWrapper<MergeProduct> {

    @Inject
    private MergeProduct mergeProduct;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(MergeProduct.BEAN_NAME);
    }

    @Override
    protected MergeProduct getWrapperStep() {
        return mergeProduct;
    }

}
