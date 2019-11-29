package com.latticeengines.cdl.workflow.steps.process;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProfileContactActivityMetricsStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;


@Component("profileContactActivityMetricsWrapper")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileContactActivityMetricsWrapper extends BaseTransformationWrapper<ProfileContactActivityMetricsStepConfiguration, ProfileContactActivityMetricsStep> {

    @Inject
    private ProfileContactActivityMetricsStep profileContactActivityMetricsStep;

    @Override
    protected ProfileContactActivityMetricsStep getWrapperStep() {
        return profileContactActivityMetricsStep;
    }

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProfileContactActivityMetricsStep.BEAN_NAME);
    }
}
