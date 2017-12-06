package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("rebuildAccountWrapper")
public class ProfileAccountWrapper extends BaseTransformationWrapper<ProfileAccount> {

    @Inject
    private ProfileAccount profileAccount;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProfileAccount.BEAN_NAME);
    }

    @Override
    protected ProfileAccount getWrapperStep() {
        return profileAccount;
    }

}
