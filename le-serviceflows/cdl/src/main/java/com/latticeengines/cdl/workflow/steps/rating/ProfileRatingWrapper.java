package com.latticeengines.cdl.workflow.steps.rating;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("profileRatingWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileRatingWrapper extends BaseTransformationWrapper<ProcessRatingStepConfiguration, ProfileRating> {

    @Inject
    private ProfileRating profileRating;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProfileRating.BEAN_NAME);
    }

    @Override
    protected ProfileRating getWrapperStep() {
        return profileRating;
    }

}
