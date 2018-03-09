package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("profileAccountWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileAccountWrapper extends BaseTransformationWrapper<ProcessAccountStepConfiguration, ProfileAccount> {

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
