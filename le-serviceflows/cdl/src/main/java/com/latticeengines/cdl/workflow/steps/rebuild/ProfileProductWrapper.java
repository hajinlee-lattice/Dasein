package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("profileProductWrapper")
public class ProfileProductWrapper extends BaseTransformationWrapper<ProfileProduct> {

    @Inject
    private ProfileProduct profileProduct;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProfileProduct.BEAN_NAME);
    }

    @Override
    protected ProfileProduct getWrapperStep() {
        return profileProduct;
    }

}
