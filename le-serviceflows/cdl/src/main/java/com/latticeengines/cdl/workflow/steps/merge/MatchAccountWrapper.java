package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("MatchAccountWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchAccountWrapper extends BaseTransformationWrapper<ProcessAccountStepConfiguration, MatchAccount> {

    @Inject
    private MatchAccount matchAccount;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(MatchAccount.BEAN_NAME);
    }

    @Override
    protected MatchAccount getWrapperStep() {
        return matchAccount;
    }

}
