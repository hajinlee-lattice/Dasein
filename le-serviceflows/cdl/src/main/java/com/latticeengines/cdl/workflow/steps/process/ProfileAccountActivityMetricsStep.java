package com.latticeengines.cdl.workflow.steps.process;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProfileAccountActivityMetricsStepConfiguration;


@Component("profileAccountActivityMetricsStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class ProfileAccountActivityMetricsStep extends ProfileActivityMetricsStepBase<ProfileAccountActivityMetricsStepConfiguration> {

    static final String BEAN_NAME = "profileAccountActivityMetricsStep";

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.Account;
    }

    @Override
    protected String getRequestName() {
        return "ProfileAccountActivityMetrics";
    }

    @Override
    public ProfileAccountActivityMetricsStepConfiguration getConfiguration() {
        return configuration;
    }
}
