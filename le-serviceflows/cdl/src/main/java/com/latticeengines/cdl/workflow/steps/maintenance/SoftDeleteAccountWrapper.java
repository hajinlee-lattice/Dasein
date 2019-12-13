package com.latticeengines.cdl.workflow.steps.maintenance;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("SoftDeleteAccountWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SoftDeleteAccountWrapper extends BaseTransformationWrapper<ProcessAccountStepConfiguration, SoftDeleteAccount> {

    @Inject
    private SoftDeleteAccount softDeleteAccount;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(SoftDeleteAccount.BEAN_NAME);
    }

    @Override
    protected SoftDeleteAccount getWrapperStep() {
        return softDeleteAccount;
    }
}
