package com.latticeengines.cdl.workflow.steps.maintenance;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("SoftDeleteTransactionWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SoftDeleteTransactionWrapper extends BaseTransformationWrapper<ProcessTransactionStepConfiguration, SoftDeleteTransaction> {

    @Inject
    private SoftDeleteTransaction softDeleteTransaction;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(SoftDeleteTransaction.BEAN_NAME);
    }

    @Override
    protected SoftDeleteTransaction getWrapperStep() {
        return softDeleteTransaction;
    }
}
