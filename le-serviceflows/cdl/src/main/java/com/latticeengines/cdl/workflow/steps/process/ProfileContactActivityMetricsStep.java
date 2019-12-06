package com.latticeengines.cdl.workflow.steps.process;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProfileContactActivityMetricsStepConfiguration;


@Component("profileContactActivityMetricsStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class ProfileContactActivityMetricsStep extends ProfileActivityMetricsStepBase<ProfileContactActivityMetricsStepConfiguration> {

    static final String BEAN_NAME = "profileContactActivityMetricsStep";

    @Override
    protected BusinessEntity getEntityLevel() {
        return BusinessEntity.Contact;
    }

    @Override
    protected String getRequestName() {
        return "ProfileContactActivityMetrics";
    }

    @Override
    public ProfileContactActivityMetricsStepConfiguration getConfiguration() {
        return configuration;
    }
}
