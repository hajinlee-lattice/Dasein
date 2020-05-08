package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedContactAttributesStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("profileCuratedContactWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileCuratedContactWrapper
        extends BaseTransformationWrapper<CuratedContactAttributesStepConfiguration, ProfileCuratedContact> {

    @Inject
    private ProfileCuratedContact profileCuratedContact;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProfileCuratedContact.BEAN_NAME);
    }

    @Override
    protected ProfileCuratedContact getWrapperStep() {
        return profileCuratedContact;
    }

}
