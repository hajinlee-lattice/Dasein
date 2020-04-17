package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedAccountAttributesStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("profileCuratedAccountWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileCuratedAccountWrapper
        extends BaseTransformationWrapper<CuratedAccountAttributesStepConfiguration, ProfileCuratedAccount> {

    @Inject
    private ProfileCuratedAccount profileCuratedAccount;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProfileCuratedAccount.BEAN_NAME);
    }

    @Override
    protected ProfileCuratedAccount getWrapperStep() {
        return profileCuratedAccount;
    }

}
