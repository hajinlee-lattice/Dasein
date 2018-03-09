package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("profilePurchaseHistoryWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfilePurchaseHistoryWrapper
        extends BaseTransformationWrapper<ProcessTransactionStepConfiguration, ProfilePurchaseHistory> {

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
