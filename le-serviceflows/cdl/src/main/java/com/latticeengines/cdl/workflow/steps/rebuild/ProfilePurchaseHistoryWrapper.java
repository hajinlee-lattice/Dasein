package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("profilePurchaseHistoryWrapper")
public class ProfilePurchaseHistoryWrapper extends BaseTransformationWrapper<ProfilePurchaseHistory> {

    @Inject
    private ProfilePurchaseHistory profilePurchaseHistory;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProfilePurchaseHistory.BEAN_NAME);
    }

    @Override
    protected ProfilePurchaseHistory getWrapperStep() {
        return profilePurchaseHistory;
    }

}
