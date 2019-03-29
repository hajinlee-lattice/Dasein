package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("rematchAccountWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RematchAccountWrapper extends BaseTransformationWrapper<ProcessAccountStepConfiguration, RematchAccount> {

    @Inject
    private RematchAccount rematchAccount;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(RematchAccount.BEAN_NAME);
    }

    @Override
    protected RematchAccount getWrapperStep() {
        return rematchAccount;
    }

}
