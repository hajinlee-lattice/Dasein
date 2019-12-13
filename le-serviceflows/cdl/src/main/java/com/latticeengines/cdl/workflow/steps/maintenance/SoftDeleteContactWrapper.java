package com.latticeengines.cdl.workflow.steps.maintenance;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("SoftDeleteContactWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SoftDeleteContactWrapper extends BaseTransformationWrapper<ProcessContactStepConfiguration, SoftDeleteContact> {

    @Inject
    private SoftDeleteContact softDeleteContact;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(SoftDeleteContact.BEAN_NAME);
    }

    @Override
    protected SoftDeleteContact getWrapperStep() {
        return softDeleteContact;
    }
}
