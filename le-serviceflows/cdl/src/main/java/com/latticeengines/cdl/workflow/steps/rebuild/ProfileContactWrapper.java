package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("profileContactWrapper")
public class ProfileContactWrapper extends BaseTransformationWrapper<ProfileContact> {

    @Inject
    private ProfileContact profileContact;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProfileContact.BEAN_NAME);
    }

    @Override
    protected ProfileContact getWrapperStep() {
        return profileContact;
    }

}
