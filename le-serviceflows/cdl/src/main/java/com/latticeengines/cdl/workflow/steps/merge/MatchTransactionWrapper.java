package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("MatchTransactionWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchTransactionWrapper
        extends BaseTransformationWrapper<ProcessTransactionStepConfiguration, MatchTransaction> {

    @Inject
    private MatchTransaction matchTransaction;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(MatchTransaction.BEAN_NAME);
    }

    @Override
    protected MatchTransaction getWrapperStep() {
        return matchTransaction;
    }
}
