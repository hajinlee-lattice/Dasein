package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("profileProductWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileProductWrapper extends BaseTransformationWrapper<ProcessProductStepConfiguration, ProfileProduct> {

    @Inject
    private ProfileProduct profileProduct;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProfileProduct.BEAN_NAME);
    }

    @Override
    protected ProfileProduct getWrapperStep() {
        return profileProduct;
    }

}
