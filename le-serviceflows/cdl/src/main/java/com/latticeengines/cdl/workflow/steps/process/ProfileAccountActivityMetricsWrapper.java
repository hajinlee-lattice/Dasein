package com.latticeengines.cdl.workflow.steps.process;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProfileAccountActivityMetricsStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;


@Component("profileAccountActivityMetricsWrapper")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileAccountActivityMetricsWrapper extends BaseTransformationWrapper<ProfileAccountActivityMetricsStepConfiguration, ProfileAccountActivityMetricsStep> {

    @Inject
    private ProfileAccountActivityMetricsStep profileAccountActivityMetricsStep;

    @Override
    protected ProfileAccountActivityMetricsStep getWrapperStep() {
        return profileAccountActivityMetricsStep;
    }

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProfileAccountActivityMetricsStep.BEAN_NAME);
    }
}
