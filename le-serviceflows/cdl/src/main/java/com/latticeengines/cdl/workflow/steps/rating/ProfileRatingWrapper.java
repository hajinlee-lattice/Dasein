package com.latticeengines.cdl.workflow.steps.rating;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("profileRatingWrapper")
public class ProfileRatingWrapper extends BaseTransformationWrapper<ProfileRating> {

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
