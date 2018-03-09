package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("profileContactWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileContactWrapper extends BaseTransformationWrapper<ProcessContactStepConfiguration, ProfileContact> {

    @Inject
    private ProfileContact profileContact;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProfileContact.BEAN_NAME);
    }

    @Override
    protected ProfileContact getWrapperStep() {
        return profileContact;
    }

}
