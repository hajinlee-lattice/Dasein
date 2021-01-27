package com.latticeengines.cdl.workflow.steps.integration;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.integration.AggregateBrokerFileConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("aggregateBrokerFile")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AggregateBrokerFile extends BaseWorkflowStep<AggregateBrokerFileConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(AggregateBrokerFile.class);

    @Override
    public void execute() {
    }
}
